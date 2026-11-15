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

package org.apache.cassandra.metrics;

import java.util.Arrays;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import org.apache.cassandra.utils.Pair;

/**
 * A Reservoir-like class that tracks the last N paired measurements.
 */
public class PairedSlidingWindowReservoir
{
    private final int windowSize;
    private long count;
    // Readers take the published snapshot without the lock, so update() replaces it instead of changing its array.
    private volatile PairedSnapshot snapshot = new PairedSnapshot(new IntIntPair[0]);

    public PairedSlidingWindowReservoir(int size)
    {
        Preconditions.checkArgument(size > 0, "Window size must be positive, got %s", size);
        this.windowSize = size;
        this.count = 0L;
    }

    public int size()
    {
        return this.snapshot.size();
    }

    public synchronized void update(int value1, int value2)
    {
        var values = Arrays.copyOf(this.snapshot.values, (int) Math.min(this.count + 1, this.windowSize));
        values[(int) (this.count % this.windowSize)] = new IntIntPair(value1, value2);
        this.snapshot = new PairedSnapshot(values);
        this.count++;
    }

    public PairedSnapshot getSnapshot()
    {
        return this.snapshot;
    }

    /**
     * A pair of ints.  "y" and "x" are used to imply that the first value is the
     * dependent one for a LinearFit computation.
     */
    public static class IntIntPair
    {
        public final int x;
        public final int y;

        IntIntPair(int x, int y)
        {
            this.y = y;
            this.x = x;
        }

        @Override
        public String toString()
        {
            return String.format("(%d,%d)", x, y);
        }
    }

    public static class PairedSnapshot
    {
        private final IntIntPair[] values;
        private final Pair<Double, Double> interceptSlope;

        private PairedSnapshot(IntIntPair[] values)
        {
            this.values = values;
            this.interceptSlope = LinearFit.interceptSlopeFor(values);
        }

        public int size()
        {
            return values.length;
        }

        /**
         * A copy of the pairs, in slot order.
         */
        @VisibleForTesting
        public IntIntPair[] values()
        {
            return values.clone();
        }

        /**
         * The intercept and slope of the best linear fit to the pairs, see {@link LinearFit#interceptSlopeFor}.
         */
        public Pair<Double, Double> interceptSlope()
        {
            return interceptSlope;
        }
    }
}
