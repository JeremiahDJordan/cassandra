/*
 * Copyright DataStax, Inc.
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

package org.apache.cassandra.exceptions;
import java.io.IOException;

import org.junit.Test;

import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.schema.TableId;

import static org.apache.cassandra.net.MessagingService.VERSION_30;
import static org.apache.cassandra.net.MessagingService.VERSION_3014;
import static org.apache.cassandra.net.MessagingService.VERSION_40;
import static org.apache.cassandra.net.MessagingService.VERSION_41;
import static org.apache.cassandra.net.MessagingService.VERSION_DS_10;
import static org.apache.cassandra.net.MessagingService.VERSION_DS_11;
import static org.apache.cassandra.net.MessagingService.VERSION_DS_12;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class RequestFailureReasonTest
{
    private static final RequestFailureReason[] REASONS = RequestFailureReason.values();
    private static final Object[][] EXPECTED_VALUES =
    {
    { 0, "UNKNOWN" },
    { 1, "READ_TOO_MANY_TOMBSTONES" },
    { 2, "TIMEOUT" },
    { 3, "INCOMPATIBLE_SCHEMA" },
    { 6, "INDEX_NOT_AVAILABLE" },
    { 12, "TRUNCATE_FAILED" },
    { 500, "UNKNOWN_COLUMN" },
    { 501, "UNKNOWN_TABLE" },
    { 502, "REMOTE_STORAGE_FAILURE" },
    { 503, "INDEX_BUILD_IN_PROGRESS" },
    { 504, "FEATURE_NEEDS_INDEX_REBUILD" }
    };
    @Test
    public void testEnumCodesAndNames()
    {
        for (int i = 0; i < REASONS.length; i++)
        {
            assertEquals("RequestFailureReason code mismatch for " +
                         REASONS[i].name(), EXPECTED_VALUES[i][0], REASONS[i].code);
            assertEquals("RequestFailureReason name mismatch for code " +
                         REASONS[i].code, EXPECTED_VALUES[i][1], REASONS[i].name());
        }
        assertEquals("Number of RequestFailureReason enum constants has changed. Update the test.",
                     EXPECTED_VALUES.length, REASONS.length);
    }

    @Test
    public void testFromCode()
    {
        // Test valid codes
        for (Object[] expected : EXPECTED_VALUES)
        {
            int code = (Integer) expected[0];
            RequestFailureReason expectedReason = RequestFailureReason.valueOf((String) expected[1]);
            assertEquals(expectedReason, RequestFailureReason.fromCode(code));
        }

        // Test invalid codes
        assertEquals(RequestFailureReason.UNKNOWN, RequestFailureReason.fromCode(200));
        assertEquals(RequestFailureReason.UNKNOWN, RequestFailureReason.fromCode(999));
        assertThrows(IllegalArgumentException.class, () -> RequestFailureReason.fromCode(-1));

        // Below codes will map to UKNOWN until we rebase on the newer Apache Cassandra version, where they are not UNKNOWN.
        // We leave them UNKNOWN for now to prevent future conflicts with Apache
        assertEquals(RequestFailureReason.UNKNOWN, RequestFailureReason.fromCode(4));
        assertEquals(RequestFailureReason.UNKNOWN, RequestFailureReason.fromCode(5));
        assertEquals(RequestFailureReason.UNKNOWN, RequestFailureReason.fromCode(7));
    }

    @Test
    public void testSchemaReasonsForApache() throws IOException
    {
        for (int version : new int[]{ VERSION_30, VERSION_3014, VERSION_40, VERSION_41 })
        {
            assertEquals(RequestFailureReason.INCOMPATIBLE_SCHEMA, roundTrip(RequestFailureReason.UNKNOWN_COLUMN, version));
            assertEquals(RequestFailureReason.INCOMPATIBLE_SCHEMA, roundTrip(RequestFailureReason.UNKNOWN_TABLE, version));
            assertEquals(RequestFailureReason.INDEX_BUILD_IN_PROGRESS, roundTrip(RequestFailureReason.INDEX_BUILD_IN_PROGRESS, version));
        }
        for (int version : new int[]{ VERSION_DS_10, VERSION_DS_11, VERSION_DS_12 })
        {
            assertEquals(RequestFailureReason.UNKNOWN_COLUMN, roundTrip(RequestFailureReason.UNKNOWN_COLUMN, version));
            assertEquals(RequestFailureReason.UNKNOWN_TABLE, roundTrip(RequestFailureReason.UNKNOWN_TABLE, version));
        }
    }

    private static RequestFailureReason roundTrip(RequestFailureReason reason, int version) throws IOException
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            RequestFailureReason.serializer.serialize(reason, out, version);
            assertEquals(out.getLength(), RequestFailureReason.serializer.serializedSize(reason, version));
            try (DataInputBuffer in = new DataInputBuffer(out.getData()))
            {
                return RequestFailureReason.serializer.deserialize(in, version);
            }
        }
    }

    @Test
    public void testExceptionSubclassMapping()
    {
        // Create a subclass of UnknownTableException
        class CustomUnknownTableException extends UnknownTableException 
        {
            public CustomUnknownTableException()
            {
                super("ks", TableId.generate());
            }
        }

        // Verify the subclass maps correctly
        // `UnknownTableException` extends` IncompatibleSchemaException`
        RequestFailureReason result = RequestFailureReason.forException(new CustomUnknownTableException());
        assertTrue("Expected either UNKNOWN_TABLE or INCOMPATIBLE_SCHEMA but got " + result,
                   result == RequestFailureReason.UNKNOWN_TABLE || 
                   result == RequestFailureReason.INCOMPATIBLE_SCHEMA);
        
        // Verify the parent class still maps correctly
        assertEquals(RequestFailureReason.UNKNOWN_TABLE,
                     RequestFailureReason.forException(new UnknownTableException("ks", TableId.generate())));
        
        // Test unmapped exception returns UNKNOWN
        assertEquals(RequestFailureReason.UNKNOWN, 
                     RequestFailureReason.forException(new RuntimeException("test")));
    }
}
