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

package org.apache.cassandra.index;

import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.Throwables;

/// Collects the failures of index events that are delivered one at a time, so that one failing indexer does not stop
/// delivery to the others. [#maybeFail] then throws the first failure, with the later ones suppressed.
///
/// Not thread-safe; use one instance per delivery.
public final class IndexerFailures
{
    private static final Logger logger = LoggerFactory.getLogger(IndexerFailures.class);
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 1, TimeUnit.MINUTES);

    /// The most failures kept as suppressed exceptions of the first one. An indexer that fails on every event would
    /// otherwise add one per event. The cap applies to the exception, so an indexer that throws one cached instance
    /// does not grow it across writes either.
    static final int MAX_SUPPRESSED = 10;
    /// How far to follow a failure's causes when checking whether the first failure caused it.
    private static final int MAX_CAUSE_DEPTH = 32;

    /// The table whose indexes fail, for the log, or null if unknown.
    @Nullable
    private final ColumnFamilyStore table;
    private Throwable first;
    private int dropped;

    public IndexerFailures(@Nullable ColumnFamilyStore table)
    {
        this.table = table;
    }

    /// Records a failure. Rethrows it at once if it is an `OutOfMemoryError` (the JVM must handle it), and applies
    /// the disk failure policy. Nothing else ends the delivery.
    public void add(Throwable t)
    {
        try
        {
            JVMStabilityInspector.inspectThrowable(t);
        }
        catch (OutOfMemoryError e)
        {
            throw e;
        }
        catch (Throwable inspection)
        {
            // Anything else the inspection throws (it reports an interrupt as an UncheckedInterruptedException) must
            // not end the delivery. The failure itself is recorded below.
        }

        if (first == null)
        {
            first = t;
            return;
        }

        // The same instance can be thrown more than once, and addSuppressed rejects suppressing an exception by
        // itself. Nor can a failure caused by the first one be suppressed by it: that would make a cycle, which
        // inspecting a later failure would follow until it overflows the stack.
        if (isCausedByFirst(t))
            return;

        Throwable[] held = first.getSuppressed();
        for (Throwable s : held)
            if (s == t)
                return;

        if (held.length < MAX_SUPPRESSED)
            first.addSuppressed(t);
        else
            ++dropped;
    }

    private boolean isCausedByFirst(Throwable t)
    {
        int depth = 0;
        for (Throwable c = t; c != null && depth < MAX_CAUSE_DEPTH; c = c.getCause(), ++depth)
            if (c == first)
                return true;
        return false;
    }

    /// Throws the first recorded failure, if any.
    public void maybeFail()
    {
        if (first == null)
            return;

        if (dropped > 0)
            noSpamLogger.warn("{} more index failures in the same update of {} were not recorded as suppressed " +
                              "exceptions of {}",
                              dropped, table == null ? "an unknown table" : table, first.toString());
        Throwables.maybeFail(first);
    }
}
