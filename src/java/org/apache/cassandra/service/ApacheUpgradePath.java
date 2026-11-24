/*
 * Copyright IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.IEndpointStateChangeSubscriber;
import org.apache.cassandra.gms.PeerCheckCache;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.StorageCompatibilityMode;

import static org.apache.cassandra.utils.StorageCompatibilityMode.CASSANDRA_5;
import static org.apache.cassandra.utils.StorageCompatibilityMode.NONE;
import static org.apache.cassandra.utils.StorageCompatibilityMode.UPGRADING;

/**
 * The upgrade of a cluster from Apache Cassandra 5.0, through storage_compatibility_mode CASSANDRA_5, then UPGRADING,
 * then NONE.
 * <p>
 * A node is on the path, refusing schema changes Apache doesn't accept and keeping off the features it doesn't have,
 * while it runs in CASSANDRA_5, or runs in UPGRADING after CASSANDRA_5 and a ring member runs Apache Cassandra 5.0 or
 * CASSANDRA_5 (messaging version 13 and a 5.x release) or has an unknown version. While it is on the path or a ring
 * member runs at 13, it computes its schema version over Apache's schema columns and writes none of the DataStax-only
 * ones, so that every node agrees on it.
 * <p>
 * Each node records in {@link #FILE} of its metadata directory the last of CASSANDRA_4, HCD_1 and CASSANDRA_5 it
 * started in, keeps it in UPGRADING and removes it in NONE. It advertises its mode and that record in
 * {@link #GOSSIP_STATE} as {@code mode=<mode>;compat=<mode or none>}.
 */
public final class ApacheUpgradePath implements IEndpointStateChangeSubscriber
{
    /** The file of the metadata directory that holds the last compatibility mode the node started in. */
    public static final String FILE = "last_compatibility_mode";

    /** The gossip state that holds the node's modes, a padding state that no release reads. */
    public static final ApplicationState GOSSIP_STATE = ApplicationState.X10;

    /** The Apache Cassandra major release this line's path is compatible with. */
    private static final String TARGET = "5";

    private static final String NO_MODE = "none";

    /**
     * The modes a node advertises: its storage_compatibility_mode and the last compatibility mode it started in.
     */
    @VisibleForTesting
    static final class Modes
    {
        @Nullable
        final String mode;
        @Nullable
        final String compat;

        Modes(@Nullable String mode, @Nullable String compat)
        {
            this.mode = mode;
            this.compat = compat;
        }

        /**
         * The modes in {@code value}, or null if there is none. Unknown keys are ignored.
         */
        @Nullable
        static Modes parse(@Nullable String value)
        {
            if (value == null)
                return null;

            Map<String, String> entries = new HashMap<>();
            for (String entry : value.split(";"))
            {
                int separator = entry.indexOf('=');
                if (separator > 0)
                    entries.put(entry.substring(0, separator).trim(), entry.substring(separator + 1).trim());
            }
            return new Modes(entries.get("mode"), entries.get("compat"));
        }

        String value()
        {
            return "mode=" + mode + ";compat=" + compat;
        }
    }

    public static final ApacheUpgradePath instance = new ApacheUpgradePath();

    @Nullable
    private volatile StorageCompatibilityMode lastCompatibilityMode;
    private volatile boolean recordChanged;

    // whether a ring member runs at messaging version 13 with a 5.x release
    private volatile boolean apachePeer;

    // whether a ring member runs at 13 with a 5.x release, or has an unknown version
    private volatile boolean apacheOrUnknownPeer;

    private ApacheUpgradePath()
    {
    }

    /**
     * Whether this node is on the path: it runs in CASSANDRA_5, or in UPGRADING after CASSANDRA_5 beside a ring member
     * that runs at messaging version 13 with a 5.x release, or has an unknown version.
     */
    public static boolean isActive()
    {
        StorageCompatibilityMode mode = mode();
        return mode == CASSANDRA_5 || (mode == UPGRADING && instance.lastCompatibilityMode == CASSANDRA_5 && instance.apacheOrUnknownPeer);
    }

    /**
     * Whether this node computes its schema version over Apache's schema columns and writes none of the DataStax-only
     * ones: it is on the path, or a ring member runs at messaging version 13 with a 5.x release.
     */
    public static boolean usesApacheSchemaColumns()
    {
        return isActive() || instance.apachePeer;
    }

    @Nullable
    private static StorageCompatibilityMode recordAfterStart(StorageCompatibilityMode mode, @Nullable StorageCompatibilityMode previous)
    {
        switch (mode)
        {
            case UPGRADING:
                return previous;
            case NONE:
                return null;
            default:
                return mode;
        }
    }

    /**
     * Reads the record of the last compatibility mode at startup, before the schema loads. A node in CASSANDRA_5 doesn't
     * start if its release isn't a 5.x one, which Apache Cassandra 5.0 nodes don't pull schema from.
     */
    public synchronized void initialize()
    {
        StorageCompatibilityMode mode = mode();
        if (mode == CASSANDRA_5 && !FBUtilities.getReleaseVersionString().startsWith(TARGET + '.'))
            throw new ConfigurationException(String.format("storage_compatibility_mode %s needs a release version starting with %s., which " +
                                                           "Apache Cassandra 5.0 nodes pull schema from; this release is %s",
                                                           CASSANDRA_5, TARGET, FBUtilities.getReleaseVersionString()),
                                             false);

        File file = file();
        StorageCompatibilityMode previous = mode == NONE || !file.exists() ? null : read(file);
        lastCompatibilityMode = recordAfterStart(mode, previous);
        recordChanged = lastCompatibilityMode != previous || (lastCompatibilityMode == null && file.exists());
        // the versions of the ring members are unknown until gossip starts, so a node in UPGRADING after CASSANDRA_5
        // loads its schema with Apache's schema columns
        apacheOrUnknownPeer = true;
    }

    private static File file()
    {
        return new File(DatabaseDescriptor.getMetadataDirectory(), FILE);
    }

    private static StorageCompatibilityMode read(File file)
    {
        String value = String.join("", FileUtils.readLines(file)).trim();
        try
        {
            return StorageCompatibilityMode.valueOf(value);
        }
        catch (IllegalArgumentException e)
        {
            throw new ConfigurationException(String.format("%s holds '%s', not a storage_compatibility_mode", file, value), false);
        }
    }

    /**
     * Checks the nodes seen in the shadow round of a starting node in CASSANDRA_5, apart from those that have left or
     * been removed. It doesn't start beside a peer above messaging version 13 that doesn't run in UPGRADING after
     * CASSANDRA_5, beside a peer of a 4.x release that speaks 13, or when nodes run above 13 and none, this node's last
     * start included, runs at 13 with a 5.x release: the path has ended.
     */
    public void onShadowRound(Map<InetAddressAndPort, EndpointState> peers)
    {
        if (mode() != CASSANDRA_5)
            return;

        InetAddressAndPort local = FBUtilities.getBroadcastAddressAndPort();
        List<String> refusals = new ArrayList<>();
        boolean above13 = false;
        boolean apache = false;
        for (Map.Entry<InetAddressAndPort, EndpointState> entry : peers.entrySet())
        {
            EndpointState peer = entry.getValue();
            String status = Gossiper.getGossipStatus(peer);
            if (status.equals(VersionedValue.STATUS_LEFT) || status.equals(VersionedValue.REMOVED_TOKEN))
                continue;

            Integer version = messagingVersion(peer);
            String release = value(peer, ApplicationState.RELEASE_VERSION);
            if (version != null && version == MessagingService.VERSION_50 && release != null && release.startsWith(TARGET + '.'))
                apache = true;
            if (entry.getKey().equals(local))
                continue;

            Modes modes = Modes.parse(value(peer, GOSSIP_STATE));
            if (version != null && version > MessagingService.VERSION_50)
            {
                above13 = true;
                if (modes == null || !UPGRADING.name().equals(modes.mode) || !CASSANDRA_5.name().equals(modes.compat))
                    refusals.add(String.format("%s runs messaging version %d in storage_compatibility_mode %s, last compatibility mode %s",
                                               entry.getKey(), version, modes == null ? "unknown" : modes.mode, modes == null ? "unknown" : modes.compat));
            }
            else if (version != null && version == MessagingService.VERSION_50 && release != null && release.startsWith("4."))
            {
                refusals.add(entry.getKey() + " runs release " + release);
            }
        }
        if (above13 && !apache)
            refusals.add("the Apache upgrade path has ended, as no node runs at messaging version " + MessagingService.VERSION_50 + "; start this node in " + UPGRADING);

        if (!refusals.isEmpty())
            throw new ConfigurationException(String.format("storage_compatibility_mode %s can't join this cluster: %s",
                                                           CASSANDRA_5, String.join("; ", refusals)),
                                             false);
    }

    /**
     * Writes the record of the last compatibility mode, once the start refusals have passed, adds this node's modes to
     * the gossip states it starts with, and decides the path from the ring it has loaded.
     */
    public void beforeGossipStart(Map<ApplicationState, VersionedValue> states)
    {
        StorageCompatibilityMode compat = lastCompatibilityMode;
        if (recordChanged)
        {
            if (compat == null)
                file().deleteIfExists();
            else
                FileUtils.writeAtomically(file(), Collections.singletonList(compat.name()));
            recordChanged = false;
        }
        Modes modes = new Modes(mode().name(), compat == null ? NO_MODE : compat.name());
        states.put(GOSSIP_STATE, StorageService.instance.valueFactory.compatibilityModes(modes.value()));
        refresh(null);
    }

    @Override
    public void onJoin(InetAddressAndPort endpoint, EndpointState epState)
    {
        refresh(null);
    }

    @Override
    public void onChange(InetAddressAndPort endpoint, ApplicationState state, VersionedValue value)
    {
        switch (state)
        {
            case NET_VERSION:
            case RELEASE_VERSION:
            case STATUS:
            case STATUS_WITH_PORT:
                refresh(null);
        }
    }

    @Override
    public void onRemove(InetAddressAndPort endpoint)
    {
        // gossip removes an endpoint before the ring does
        refresh(endpoint);
    }

    /**
     * Decides the path again (see {@link #decide}). A change of the schema columns recomputes the schema version, off the
     * gossip stage; a change of the path makes the feature checks of requests look at the nodes again.
     */
    private synchronized void refresh(@Nullable InetAddressAndPort removed)
    {
        boolean wasActive = isActive();
        boolean usedApacheSchemaColumns = usesApacheSchemaColumns();
        decide(removed);
        if (wasActive != isActive())
            PeerCheckCache.peersChanged();
        if (usedApacheSchemaColumns != usesApacheSchemaColumns())
            Stage.MIGRATION.execute(Schema.instance::reloadSchemaAndAnnounceVersion);
    }

    /**
     * Decides, from the versions of the ring members other than {@code removed}, whether a member runs at messaging
     * version 13 with a 5.x release, and whether one does or has an unknown version.
     */
    private void decide(@Nullable InetAddressAndPort removed)
    {
        boolean apache = false;
        boolean unknown = false;
        InetAddressAndPort local = FBUtilities.getBroadcastAddressAndPort();
        for (InetAddressAndPort endpoint : StorageService.instance.getTokenMetadata().getAllEndpoints())
        {
            if (endpoint.equals(local) || endpoint.equals(removed))
                continue;

            EndpointState peer = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
            Integer version = peer == null ? null : messagingVersion(peer);
            String release = peer == null ? null : value(peer, ApplicationState.RELEASE_VERSION);
            if (version == null || release == null)
                unknown = true;
            else if (version == MessagingService.VERSION_50 && release.startsWith(TARGET + '.'))
                apache = true;
        }
        apachePeer = apache;
        apacheOrUnknownPeer = apache || unknown;
    }

    private static StorageCompatibilityMode mode()
    {
        return DatabaseDescriptor.getStorageCompatibilityMode();
    }

    @Nullable
    private static Integer messagingVersion(EndpointState peer)
    {
        String version = value(peer, ApplicationState.NET_VERSION);
        try
        {
            return version == null ? null : Integer.valueOf(version);
        }
        catch (NumberFormatException e)
        {
            return null;
        }
    }

    @Nullable
    private static String value(EndpointState peer, ApplicationState applicationState)
    {
        VersionedValue value = peer.getApplicationState(applicationState);
        return value == null ? null : value.value;
    }
}
