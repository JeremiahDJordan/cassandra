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

package org.apache.cassandra.io.sstable.format;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.utils.JVMStabilityInspector;

/**
 * Coordination state between ZCS and SAI index rebuilds for a single logical SSTable.
 *
 * <p>Multiple entire-SSTable streams of the same SSTable can run concurrently ({@code ZCS_STREAMING} with a
 * reference count), but a rebuild is exclusive ({@code REBUILDING}) and mutually exclusive with streaming.
 */
@ThreadSafe
public class SSTableStreamRebuildState
{
    private static final Logger logger = LoggerFactory.getLogger(SSTableStreamRebuildState.class);

    public enum State
    {
        NORMAL,
        REBUILDING,
        ZCS_STREAMING
    }

    private State state = State.NORMAL;
    private int zcsStreamCount = 0;
    // The actions to run when the state is next NORMAL (see runWhenNormal)
    private List<Runnable> whenNormal = null;

    /**
     * Attempt to begin an entire-SSTable stream.
     *
     * @return true if streaming may proceed (caller must later call {@link #endStreaming()})
     *         false if a rebuild is active and the caller should fall back to legacy streaming
     */
    public synchronized boolean tryBeginStreaming()
    {
        if (state == State.REBUILDING)
            return false;
        state = State.ZCS_STREAMING;
        zcsStreamCount++;
        return true;
    }

    public void endStreaming()
    {
        List<Runnable> actions;
        synchronized (this)
        {
            // Return to NORMAL when the last stream ends
            if (zcsStreamCount > 0 && --zcsStreamCount == 0)
                state = State.NORMAL;
            actions = takeActionsIfNormal();
        }
        run(actions);
    }

    /**
     * Attempt to begin a rebuild.
     *
     * @return true if the rebuild may proceed (caller must later call {@link #endRebuild()})
     *         false if any stream is in progress or another rebuild is already running
     */
    public synchronized boolean tryBeginRebuild()
    {
        if (state != State.NORMAL)
            return false;
        state = State.REBUILDING;
        return true;
    }

    public void endRebuild()
    {
        List<Runnable> actions;
        synchronized (this)
        {
            if (state == State.REBUILDING)
                state = State.NORMAL;
            actions = takeActionsIfNormal();
        }
        run(actions);
    }

    /**
     * Runs the provided action once neither a stream nor a rebuild holds the SSTable: right away (in the calling
     * thread) if none does, otherwise when the last one ends, in the thread that ends it. So the action should be
     * quick, e.g. schedule some work (which may then try to begin a rebuild, like any other).
     */
    public void runWhenNormal(Runnable action)
    {
        synchronized (this)
        {
            if (state != State.NORMAL)
            {
                if (whenNormal == null)
                    whenNormal = new ArrayList<>();
                whenNormal.add(action);
                return;
            }
        }
        action.run();
    }

    private List<Runnable> takeActionsIfNormal()
    {
        if (state != State.NORMAL || whenNormal == null)
            return Collections.emptyList();
        List<Runnable> actions = whenNormal;
        whenNormal = null;
        return actions;
    }

    private static void run(List<Runnable> actions)
    {
        for (Runnable action : actions)
        {
            try
            {
                action.run();
            }
            catch (Throwable t)
            {
                JVMStabilityInspector.inspectThrowable(t);
                logger.warn("Failed to run an action waiting for the end of the streams and rebuilds of an SSTable", t);
            }
        }
    }

    public synchronized State state()
    {
        return state;
    }

    @VisibleForTesting
    public synchronized int zcsStreamCount()
    {
        return zcsStreamCount;
    }
}
