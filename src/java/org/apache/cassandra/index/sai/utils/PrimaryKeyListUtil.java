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

package org.apache.cassandra.index.sai.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class PrimaryKeyListUtil
{
    /**
     * Converts sorted primary keys to the keys of the static rows of their partitions, without duplicates. An index on
     * a static column only has the static rows, so this is what it can look up for the keys selected by other indexes.
     *
     * @param keys primary keys, in primary key order
     * @return the static row keys of the partitions of {@code keys}, in primary key order
     */
    public static List<PrimaryKey> toStaticRowKeys(List<PrimaryKey> keys)
    {
        List<PrimaryKey> staticKeys = new ArrayList<>(keys.size());
        PrimaryKey last = null;
        for (PrimaryKey key : keys)
        {
            // the keys of a partition are consecutive
            if (last != null && last.partitionKey().equals(key.partitionKey()))
                continue;
            last = key.forStaticRow();
            staticKeys.add(last);
        }
        return staticKeys;
    }

    /** Create a sublist of the keys within the provided bounds (inclusive) */
    public static List<PrimaryKey> getKeysInRange(List<PrimaryKey> keys, PrimaryKey minKey, PrimaryKey maxKey)
    {
        int minIndex = PrimaryKeyListUtil.findBoundaryIndex(keys, minKey, false);
        int maxIndex = PrimaryKeyListUtil.findBoundaryIndex(keys, maxKey, true);
        return keys.subList(minIndex, maxIndex);
    }

    private static int findBoundaryIndex(List<PrimaryKey> keys, PrimaryKey key, boolean findMax)
    {
        int index = Collections.binarySearch(keys, key);

        if (index < 0)
            return ~index;

        // When findMax is true, we are finding an exclusive upper bound, but binary search is inclusive, so we
        // increment by 1 to get the exclusive upper bound.
        return findMax ? index + 1 : index;
    }
}
