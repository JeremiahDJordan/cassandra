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

package org.apache.cassandra.index.sai.disk.v2;

import java.util.Arrays;
import java.util.Random;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DiskBinarySearchTest
{
    /**
     * Every sorted array of distinct values with gaps, of up to 9 values, and every target around them: searchInt finds
     * the index of the target or -1, and searchFloor the index of the first value at least the target, or -1.
     */
    @Test
    public void testAgainstLinearSearch()
    {
        Random random = new Random(42);
        for (int size = 1; size <= 9; size++)
        {
            for (int round = 0; round < 200; round++)
            {
                int[] values = new int[size];
                int value = random.nextInt(3);
                for (int i = 0; i < size; i++)
                {
                    values[i] = value;
                    value += 1 + random.nextInt(3);
                }
                for (int low = 0; low < size; low++)
                {
                    for (int target = values[0] - 2; target <= values[size - 1] + 2; target++)
                    {
                        int exact = -1, ceiling = -1;
                        for (int i = size - 1; i >= low; i--)
                        {
                            if (values[i] == target)
                                exact = i;
                            if (values[i] >= target)
                                ceiling = i;
                        }
                        String message = "values " + Arrays.toString(values) + " from " + low + ", target " + target;
                        assertEquals(message, exact, DiskBinarySearch.searchInt(low, size, target, i -> values[(int) i]));
                        assertEquals(message, ceiling, DiskBinarySearch.searchFloor(low, size, target, i -> values[(int) i]));
                    }
                }
            }
        }
    }

    @Test
    public void testFirstValueAfterAGap()
    {
        // the row ids of the vectors of an sstable whose row 6 (e.g. a static row) has none
        int[] rowIds = { 0, 1, 2, 3, 4, 5, 7, 8 };
        assertEquals(6, DiskBinarySearch.searchFloor(0, rowIds.length, 6, i -> rowIds[(int) i]));
        assertEquals(-1, DiskBinarySearch.searchFloor(0, rowIds.length, 9, i -> rowIds[(int) i]));
        assertEquals(0, DiskBinarySearch.searchFloor(0, rowIds.length, -1, i -> rowIds[(int) i]));
    }
}
