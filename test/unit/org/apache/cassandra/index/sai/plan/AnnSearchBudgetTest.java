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

import org.junit.Test;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.filter.ANNOptions;
import org.apache.cassandra.index.sai.disk.vector.VectorCompression;
import org.apache.cassandra.index.sai.disk.vector.VectorSourceModel;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AnnSearchBudgetTest
{
    @Test
    public void fractionBoundsAndRounding()
    {
        assertEquals(10, AnnSearchBudget.applyFraction(10, 67, 0));
        assertEquals(21, AnnSearchBudget.applyFraction(10, 67, 0.30));
        assertEquals(34, AnnSearchBudget.applyFraction(10, 67, 0.50));
        assertEquals(67, AnnSearchBudget.applyFraction(10, 67, 1));
        assertEquals(50, AnnSearchBudget.applyFraction(50, 67, 0.30));
        assertEquals(Integer.MAX_VALUE, AnnSearchBudget.applyFraction(10, Integer.MAX_VALUE, 1));
    }

    @Test
    public void singleGraphPreservesAllModelBudgets()
    {
        for (VectorSourceModel model : VectorSourceModel.values())
            for (VectorCompression vc : new VectorCompression[] { VectorCompression.NO_COMPRESSION, model.compressionProvider.apply(384) })
                for (int limit : new int[] { 1, 10, 20, 50, 100, 1000 })
                    for (double fraction : new double[] { 0, 0.15, 0.30, 0.50, 0.75, 1 })
                    {
                        int budget = model.rerankKFor(limit, vc);
                        assertEquals(budget, AnnSearchBudget.applyFraction(budget, budget, fraction));
                    }
    }

    @Test
    public void minimumScalesWithQueryLimitAndCompression()
    {
        for (VectorSourceModel model : VectorSourceModel.values())
            for (VectorCompression vc : new VectorCompression[] { VectorCompression.NO_COMPRESSION, model.compressionProvider.apply(384) })
            {
                int previous = 0;
                for (int limit : new int[] { 10, 20, 50, 100, 1000 })
                {
                    int local = model.rerankKFor(1, vc);
                    int global = model.rerankKFor(limit, vc);
                    int budget = AnnSearchBudget.applyFraction(local, global, 0.30);
                    assertEquals(Math.max(local, (int) Math.ceil(global * 0.30)), budget);
                    assertTrue(budget >= previous);
                    assertTrue(budget <= global);
                    previous = budget;
                }
            }
    }

    @Test
    public void explicitOptionsBypassAutomaticBudget()
    {
        for (int explicit : new int[] { -1, 0, 20, 200 })
        {
            Orderer orderer = new Orderer(null, Operator.ANN, null, ANNOptions.create(explicit, null));
            assertEquals(explicit, orderer.withAnnGlobalLimit(100).rerankKFor(1, VectorCompression.NO_COMPRESSION));
        }
    }
}
