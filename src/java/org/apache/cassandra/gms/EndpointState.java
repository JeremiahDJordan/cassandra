/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.gms;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import com.google.common.base.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.CassandraVersion;

/**
 * This abstraction represents both the HeartBeatState and the ApplicationState in an EndpointState
 * instance. Any state for a given endpoint can be retrieved from this instance.
 */
public class EndpointState
{
    protected static final Logger logger = LoggerFactory.getLogger(EndpointState.class);

    public final static IVersionedSerializer<EndpointState> serializer = new EndpointStateSerializer();

    private static class View
    {
        final HeartBeatState hbState;
        final Map<ApplicationState, VersionedValue> applicationState;

        private View(HeartBeatState hbState, Map<ApplicationState, VersionedValue> applicationState)
        {
            this.hbState = hbState;
            this.applicationState = applicationState;
        }
    }

    private final AtomicReference<View> ref;

    /* fields below do not get serialized */
    private volatile long updateTimestamp;
    private volatile boolean isAlive;

    public EndpointState(HeartBeatState initialHbState)
    {
        this(initialHbState, new EnumMap<>(ApplicationState.class));
    }

    public EndpointState(EndpointState other)
    {
        ref = new AtomicReference<>(other.ref.get());
        updateTimestamp = System.nanoTime();
        isAlive = true;
    }

    @VisibleForTesting
    public EndpointState(HeartBeatState initialHbState, Map<ApplicationState, VersionedValue> states)
    {
        ref = new AtomicReference<>(new View(initialHbState, new EnumMap<>(states)));
        updateTimestamp = System.nanoTime();
        isAlive = true;
    }

    @VisibleForTesting
    public HeartBeatState getHeartBeatState()
    {
        return ref.get().hbState;
    }

    public void updateHeartBeat()
    {
        updateHeartBeat(HeartBeatState::updateHeartBeat);
    }

    public void forceNewerGenerationUnsafe()
    {
        updateHeartBeat(HeartBeatState::forceNewerGenerationUnsafe);
    }

    @VisibleForTesting
    public void forceHighestPossibleVersionUnsafe()
    {
        updateHeartBeat(HeartBeatState::forceHighestPossibleVersionUnsafe);
    }

    void unsafeSetEmptyHeartBeatState()
    {
        updateHeartBeat(ignore -> HeartBeatState.empty());
    }

    private void updateHeartBeat(Function<HeartBeatState, HeartBeatState> fn)
    {
        HeartBeatState previous = null;
        HeartBeatState update = null;
        while (true)
        {
            View view = ref.get();
            if (previous == null || view.hbState != previous) // if this races with updating states then can avoid bumping versions
                update = fn.apply(view.hbState);
            if (ref.compareAndSet(view, new View(update, view.applicationState)))
                return;
            previous = view.hbState;
        }
    }

    public VersionedValue getApplicationState(ApplicationState key)
    {
        return ref.get().applicationState.get(key);
    }

    public boolean containsApplicationState(ApplicationState key)
    {
        return ref.get().applicationState.containsKey(key);
    }

    public Set<Map.Entry<ApplicationState, VersionedValue>> states()
    {
        return ref.get().applicationState.entrySet();
    }

    public void addApplicationState(ApplicationState key, VersionedValue value)
    {
        addApplicationStates(Collections.singletonMap(key, value));
    }

    public void addApplicationStates(Map<ApplicationState, VersionedValue> values)
    {
        addApplicationStates(values.entrySet());
    }

    public void addApplicationStates(Set<Map.Entry<ApplicationState, VersionedValue>> values)
    {
        addApplicationStates(values, null);
    }

    public void addApplicationStates(Set<Map.Entry<ApplicationState, VersionedValue>> values, @Nullable HeartBeatState hbState)
    {
        while (true)
        {
            View view = this.ref.get();
            Map<ApplicationState, VersionedValue> orig = view.applicationState;
            Map<ApplicationState, VersionedValue> copy = new EnumMap<>(orig);

            for (Map.Entry<ApplicationState, VersionedValue> value : values)
                copy.put(value.getKey(), value.getValue());

            if (this.ref.compareAndSet(view, new View(hbState == null ? view.hbState : hbState, copy)))
            {
                if (hbState != null)
                    updateTimestamp();
                return;
            }
        }
    }

    void removeMajorVersion3LegacyApplicationStates()
    {
        while (hasLegacyFields())
        {
            View view = ref.get();
            Map<ApplicationState, VersionedValue> orig = view.applicationState;
            Map<ApplicationState, VersionedValue> updatedStates = filterMajorVersion3LegacyApplicationStates(orig);
            // avoid updating if no state is removed
            if (orig.size() == updatedStates.size()
                || ref.compareAndSet(view, new View(view.hbState, updatedStates)))
                return;
        }
    }

    private boolean hasLegacyFields()
    {
        Set<ApplicationState> statesPresent = ref.get().applicationState.keySet();
        if (statesPresent.isEmpty())
            return false;
        return (statesPresent.contains(ApplicationState.STATUS) && statesPresent.contains(ApplicationState.STATUS_WITH_PORT))
               || (statesPresent.contains(ApplicationState.INTERNAL_IP) && statesPresent.contains(ApplicationState.INTERNAL_ADDRESS_AND_PORT))
               || (statesPresent.contains(ApplicationState.RPC_ADDRESS) && statesPresent.contains(ApplicationState.NATIVE_ADDRESS_AND_PORT));
    }

    private static Map<ApplicationState, VersionedValue> filterMajorVersion3LegacyApplicationStates(Map<ApplicationState, VersionedValue> states)
    {
        return states.entrySet().stream().filter(entry -> {
                // Filter out pre-4.0 versions of data for more complete 4.0 versions
                switch (entry.getKey())
                {
                    case INTERNAL_IP:
                        return !states.containsKey(ApplicationState.INTERNAL_ADDRESS_AND_PORT);
                    case STATUS:
                        return !states.containsKey(ApplicationState.STATUS_WITH_PORT);
                    case RPC_ADDRESS:
                        return !states.containsKey(ApplicationState.NATIVE_ADDRESS_AND_PORT);
                    default:
                        return true;
                }
            }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /* getters and setters */
    /**
     * @return System.nanoTime() when state was updated last time.
     */
    public long getUpdateTimestamp()
    {
        return updateTimestamp;
    }

    void updateTimestamp()
    {
        updateTimestamp = System.nanoTime();
    }

    public boolean isAlive()
    {
        return isAlive;
    }

    @VisibleForTesting
    public void markAlive()
    {
        boolean wasAlive = isAlive;
        isAlive = true;
        if (!wasAlive)
            PeerCheckCache.peersChanged();
    }

    @VisibleForTesting
    public void markDead()
    {
        boolean wasAlive = isAlive;
        isAlive = false;
        if (wasAlive)
            PeerCheckCache.peersChanged();
    }

    public boolean isStateEmpty()
    {
        return ref.get().applicationState.isEmpty();
    }

    /**
     * @return true if {@link HeartBeatState#isEmpty()} is true and no STATUS application state exists
     */
    public boolean isEmptyWithoutStatus()
    {
        View view = ref.get();
        Map<ApplicationState, VersionedValue> state = view.applicationState;
        boolean hasStatus = state.containsKey(ApplicationState.STATUS_WITH_PORT) || state.containsKey(ApplicationState.STATUS);
        return view.hbState.isEmpty() && !hasStatus;
    }

    public boolean isRpcReady()
    {
        VersionedValue rpcState = getApplicationState(ApplicationState.RPC_READY);
        return rpcState != null && Boolean.parseBoolean(rpcState.value);
    }

    public boolean isNormalState()
    {
        return getStatus().equals(VersionedValue.STATUS_NORMAL);
    }

    public String getStatus()
    {
        VersionedValue status = getApplicationState(ApplicationState.STATUS_WITH_PORT);
        if (status == null)
        {
            status = getApplicationState(ApplicationState.STATUS);
        }
        if (status == null)
        {
            return "";
        }

        String[] pieces = status.value.split(VersionedValue.DELIMITER_STR, -1);
        assert (pieces.length > 0);
        return pieces[0];
    }

    @Nullable
    public UUID getSchemaVersion()
    {
        VersionedValue applicationState = getApplicationState(ApplicationState.SCHEMA);
        return applicationState != null
               ? UUID.fromString(applicationState.value)
               : null;
    }

    @Nullable
    public CassandraVersion getReleaseVersion()
    {
        VersionedValue applicationState = getApplicationState(ApplicationState.RELEASE_VERSION);
        return applicationState != null
               ? new CassandraVersion(applicationState.value)
               : null;
    }

    public String toString()
    {
        View view = ref.get();
        return "EndpointState: HeartBeatState = " + view.hbState + ", AppStateMap = " + view.applicationState;
    }
}

class EndpointStateSerializer implements IVersionedSerializer<EndpointState>
{
    private static final Logger logger = LoggerFactory.getLogger(EndpointStateSerializer.class);

    public void serialize(EndpointState epState, DataOutputPlus out, int version) throws IOException
    {
        /* serialize the HeartBeatState */
        HeartBeatState hbState = epState.getHeartBeatState();
        HeartBeatState.serializer.serialize(hbState, out, version);

        /* serialize the map of ApplicationState objects */
        Set<Map.Entry<ApplicationState, VersionedValue>> states = filterOutgoingStates(epState.states(), version);
        out.writeInt(states.size());
        for (Map.Entry<ApplicationState, VersionedValue> state : states)
        {
            out.writeInt(state.getKey().ordinal());
            VersionedValue.serializer.serialize(state.getValue(), out, version);
        }
    }

    public EndpointState deserialize(DataInputPlus in, int version) throws IOException
    {
        HeartBeatState hbState = HeartBeatState.serializer.deserialize(in, version);

        int appStateSize = in.readInt();
        Map<ApplicationState, VersionedValue> states = new EnumMap<>(ApplicationState.class);
        for (int i = 0; i < appStateSize; ++i)
        {
            int key = in.readInt();
            VersionedValue value = VersionedValue.serializer.deserialize(in, version);
            states.put(Gossiper.STATES[key], value);
        }

        return new EndpointState(hbState, filterIncomingStates(states, version));
    }

    public long serializedSize(EndpointState epState, int version)
    {
        long size = HeartBeatState.serializer.serializedSize(epState.getHeartBeatState(), version);
        Set<Map.Entry<ApplicationState, VersionedValue>> states = filterOutgoingStates(epState.states(), version);
        size += TypeSizes.sizeof(states.size());
        for (Map.Entry<ApplicationState, VersionedValue> state : states)
        {
            size += TypeSizes.sizeof(state.getKey().ordinal());
            size += VersionedValue.serializer.serializedSize(state.getValue(), version);
        }
        return size;
    }

    @VisibleForTesting
    static Set<Map.Entry<ApplicationState, VersionedValue>> filterOutgoingStates(Set<Map.Entry<ApplicationState, VersionedValue>> states, int version)
    {
        if (version < MessagingService.VERSION_40 && !MessagingService.current_version_override)
        {
            Set<Map.Entry<ApplicationState, VersionedValue>> filteredStates = new HashSet<>();
            for (Map.Entry<ApplicationState, VersionedValue> state : states)
                filteredStates.addAll(filterOutgoingState(state, version).entrySet());
            return filteredStates;
        }
        return states;
    }

    private static Map<ApplicationState, VersionedValue> filterOutgoingState(Map.Entry<ApplicationState, VersionedValue> state, int version)
    {
        assert version < MessagingService.VERSION_40 && !MessagingService.current_version_override;
        VersionedValue vv = state.getValue();
        if (logger.isTraceEnabled())
            logger.trace("Fetching the key from state {}({}) with value of {}", state.getKey(), state.getKey().ordinal(), vv);
        String[] values;
        switch (state.getKey())
        {
            case INTERNAL_ADDRESS_AND_PORT:
                // DSE sends only out this value as a storage port thus it doesn't contain the address part
                // If such a state has been saved into the gossip state we cannot read the address out of it
                // instead we map it to STORAGE_PORT
                values = splitAddressAndPort(vv);
                if (values.length > 1)
                    return Map.of(ApplicationState.values()[7], VersionedValue.unsafeMakeVersionedValue(values[0], vv.version),
                                  ApplicationState.values()[17], VersionedValue.unsafeMakeVersionedValue(values[1], vv.version));
                else
                    return Map.of(ApplicationState.values()[17], VersionedValue.unsafeMakeVersionedValue(vv.value, vv.version));
            case NATIVE_ADDRESS_AND_PORT:
                // DSE sends only out this value as a native transport port thus it doesn't contain the address part
                // If such a state has been saved into the gossip state we cannot read the address out of it
                // instead we map it to NATIVE_TRANSPORT_PORT
                values = splitAddressAndPort(vv);
                if (values.length > 1)
                    return Map.of(ApplicationState.values()[15], VersionedValue.unsafeMakeVersionedValue(values[1], vv.version));
                else
                    return Map.of(ApplicationState.values()[15], VersionedValue.unsafeMakeVersionedValue(vv.value, vv.version));
            case STATUS_WITH_PORT:
                // DSE sends only out status without port
                // If such a state has been saved into the gossip state we cannot read the port out of it
                // instead we map it to STATUS
                values = splitStatusAndPort(vv);
                if (values.length > 1)
                    return Map.of(ApplicationState.values()[0], VersionedValue.unsafeMakeVersionedValue(values[0], vv.version));
                else
                    return Map.of(ApplicationState.values()[0], VersionedValue.unsafeMakeVersionedValue(vv.value, vv.version));
            case DISK_USAGE:
                return Map.of(ApplicationState.values()[21], state.getValue());
            case SSTABLE_VERSIONS:
                // has it as 18 but DSE doesn't have it at all (dse's 18 is STORAGE_PORT_SSL)
                //  as schema changes must not be performed during a major upgrade, just ignore sending this state to the legacy peers
                return Map.of();
            case INDEX_STATUS:
                return Map.of();
            case RELEASE_VERSION:
                // CC versions add a sha suffix that C* 3.x nodes cannot parse
                return Map.of(ApplicationState.RELEASE_VERSION, VersionedValue.unsafeMakeVersionedValue(vv.value.replaceFirst("-[0-9a-f]{7,40}$", ""), vv.version));
            default:
                return Map.of(state.getKey(), state.getValue());
        }
    }

    private static String[] splitStatusAndPort(VersionedValue vv)
    {
        return vv.value.split("[:,]");
    }

    private static String[] splitAddressAndPort(VersionedValue vv)
    {
        return vv.value.split(":");
    }

    @VisibleForTesting
    static Map<ApplicationState, VersionedValue> filterIncomingStates(Map<ApplicationState, VersionedValue> states, int version)
    {
        if (version < MessagingService.VERSION_40 && !MessagingService.current_version_override)
        {
            Map<ApplicationState, VersionedValue> filteredStates = new EnumMap<>(ApplicationState.class);
            for (Map.Entry<ApplicationState, VersionedValue> state : states.entrySet())
            {
                VersionedValue vv = state.getValue();
                if (logger.isTraceEnabled())
                    logger.trace("Storing the key to state {}({}) with value of {}", state.getKey(), state.getKey().ordinal(), vv);
                switch (state.getKey().ordinal())
                {
                    case 15: // NATIVE_TRANSPORT_PORT --> NATIVE_ADDRESS_AND_PORT
                        filteredStates.put(ApplicationState.NATIVE_ADDRESS_AND_PORT, vv);
                        break;
                    case 16: // NATIVE_TRANSPORT_PORT_SSL
                        break;
                    case 17: // STORAGE_PORT --> INTERNAL_ADDRESS_AND_PORT
                        filteredStates.put(ApplicationState.INTERNAL_ADDRESS_AND_PORT, vv);
                        break;
                    case 18: // STORAGE_PORT_SSL
                    case 19: // JMX_PORT
                    case 20: // SCHEMA_COMPATIBILITY_VERSION
                        break;
                    case 21:
                        filteredStates.put(ApplicationState.DISK_USAGE, vv);
                        break;
                    default:
                        filteredStates.put(state.getKey(), vv);
                }
            }
            return filteredStates;
        }
        return states;
    }
}
