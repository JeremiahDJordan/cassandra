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

package org.apache.cassandra.index.sai.utils;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;

import org.junit.Test;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import org.apache.cassandra.utils.concurrent.Ref;
import org.apache.cassandra.utils.concurrent.RefCounted;
import org.awaitility.Awaitility;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

public class ResourceLeakDetectorTest
{
    @Test
    public void testLeakFailsTest()
    {
        Statement leakingTest = new Statement()
        {
            @Override
            public void evaluate()
            {
                leakAndAwaitTidy();
                // a leak is reported after its tidy runs, on the same thread as the next leak's tidy
                leakAndAwaitTidy();
            }
        };
        Description description = Description.createTestDescription(ResourceLeakDetectorTest.class, "leakingTest");

        assertThatThrownBy(() -> new ResourceLeakDetector().apply(leakingTest, description).evaluate())
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining("Resource leaks were detected during this test");
    }

    private static void leakAndAwaitTidy()
    {
        CountDownLatch tidied = new CountDownLatch(1);
        new Ref<>(null, new RefCounted.Tidy()
        {
            @Override
            public void tidy()
            {
                tidied.countDown();
            }

            @Override
            public String name()
            {
                return "leaked";
            }
        });
        Awaitility.await().atMost(Duration.ofSeconds(30)).pollDelay(Duration.ofMillis(100)).untilAsserted(() -> {
            System.gc();
            assertEquals(0, tidied.getCount());
        });
    }
}
