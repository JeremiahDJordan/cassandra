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

package org.apache.cassandra.index.sai.plan;

import org.apache.cassandra.exceptions.ConfigurationException;

import static org.apache.cassandra.config.CassandraRelevantProperties.SAI_ANN_MIN_RERANK_FRACTION;

/** Separates a segment's expected result share from the depth needed to find accurate neighbors. */
final class AnnSearchBudget
{
    static final double MIN_RERANK_FRACTION = validateFraction(SAI_ANN_MIN_RERANK_FRACTION.getDouble());

    private AnnSearchBudget() {}

    static double validateFraction(double fraction)
    {
        if (!Double.isFinite(fraction) || fraction < 0 || fraction > 1)
            throw new ConfigurationException(SAI_ANN_MIN_RERANK_FRACTION.getKey() + " must be finite and in [0, 1]");
        return fraction;
    }

    static int applyFraction(int localBudget, int globalBudget, double fraction)
    {
        return Math.max(localBudget, (int) Math.ceil(fraction * globalBudget));
    }
}
