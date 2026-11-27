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

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

public class IndexerFailuresTest
{
    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testNoFailure()
    {
        new IndexerFailures(null).maybeFail();
    }

    @Test
    public void testFirstFailureWithOthersSuppressed()
    {
        RuntimeException first = new RuntimeException("first");
        RuntimeException second = new RuntimeException("second");
        IndexerFailures failures = new IndexerFailures(null);
        failures.add(first);
        failures.add(second);
        assertSame(first, thrown(failures));
        assertEquals(1, first.getSuppressed().length);
        assertSame(second, first.getSuppressed()[0]);
    }

    /**
     * An indexer can throw one instance more than once; it must not be suppressed by itself, or twice.
     */
    @Test
    public void testSameInstanceAddedAgain()
    {
        RuntimeException first = new RuntimeException("first");
        RuntimeException second = new RuntimeException("second");
        IndexerFailures failures = new IndexerFailures(null);
        failures.add(first);
        failures.add(first);
        failures.add(second);
        failures.add(second);
        assertSame(first, thrown(failures));
        assertEquals(1, first.getSuppressed().length);
    }

    @Test
    public void testSuppressedFailuresAreCapped()
    {
        RuntimeException first = new RuntimeException("first");
        IndexerFailures failures = new IndexerFailures(null);
        failures.add(first);
        for (int i = 0; i < 1000; ++i)
            failures.add(new RuntimeException("failure " + i));
        assertSame(first, thrown(failures));
        assertEquals(IndexerFailures.MAX_SUPPRESSED, first.getSuppressed().length);
    }

    /**
     * The cap applies to the exception: an indexer that throws one cached instance must not grow it across writes.
     */
    @Test
    public void testCapAcrossDeliveries()
    {
        RuntimeException cached = new RuntimeException("cached");
        for (int write = 0; write < 5; ++write)
        {
            IndexerFailures failures = new IndexerFailures(null);
            failures.add(cached);
            for (int i = 0; i < 5; ++i)
                failures.add(new RuntimeException("write " + write + " failure " + i));
            assertSame(cached, thrown(failures));
        }
        assertEquals(IndexerFailures.MAX_SUPPRESSED, cached.getSuppressed().length);
    }

    /**
     * A failure caused by the first one must not be suppressed by it: that would be a cycle, and inspecting a later
     * failure caused by the first one would follow it until the stack overflows.
     */
    @Test
    public void testFailureCausedByFirstIsNotSuppressed()
    {
        RuntimeException first = new RuntimeException("first");
        IndexerFailures failures = new IndexerFailures(null);
        failures.add(first);
        failures.add(new RuntimeException("wrapper", first));
        failures.add(new RuntimeException("another wrapper", first));
        RuntimeException other = new RuntimeException("other");
        failures.add(other);
        assertSame(first, thrown(failures));
        assertEquals(1, first.getSuppressed().length);
        assertSame(other, first.getSuppressed()[0]);
    }

    /**
     * An OutOfMemoryError is rethrown at once, for the JVM to handle.
     */
    @Test
    public void testOutOfMemoryErrorIsRethrown()
    {
        OutOfMemoryError oom = new OutOfMemoryError("Java heap space");
        IndexerFailures failures = new IndexerFailures(null);
        failures.add(new RuntimeException("first"));
        try
        {
            failures.add(oom);
            fail("Expected the OutOfMemoryError");
        }
        catch (OutOfMemoryError e)
        {
            assertSame(oom, e);
        }
    }

    private static Throwable thrown(IndexerFailures failures)
    {
        try
        {
            failures.maybeFail();
        }
        catch (RuntimeException e)
        {
            return e;
        }
        fail("Expected a failure");
        return null;
    }
}
