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

package org.apache.cassandra.db.guardrails;

import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.exceptions.WriteFailureException;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.utils.FBUtilities;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests the {@link Guardrails#scannedTombstones} guardrail on the read of the base rows that a write to a table with a
 * materialized view does to build the view updates.
 */
public class GuardrailViewReadTombstonesTest extends GuardrailTester
{
    private static final int WARN_THRESHOLD = 100;
    private static final int FAIL_THRESHOLD = 200;

    private int defaultWarnThreshold;
    private int defaultFailThreshold;

    @Before
    public void setThresholds()
    {
        defaultWarnThreshold = DatabaseDescriptor.getGuardrailsConfig().getTombstoneWarnThreshold();
        defaultFailThreshold = DatabaseDescriptor.getGuardrailsConfig().getTombstoneFailThreshold();
        DatabaseDescriptor.getGuardrailsConfig().setTombstonesThreshold(WARN_THRESHOLD, FAIL_THRESHOLD);
    }

    @After
    public void resetThresholds()
    {
        DatabaseDescriptor.getGuardrailsConfig().setTombstonesThreshold(defaultWarnThreshold, defaultFailThreshold);
    }

    @Test
    public void testViewReadOverWarnThreshold()
    {
        createTableWithDeletedRows(WARN_THRESHOLD + 10);

        List<String> warnings = rangeDeletionWarnings(WARN_THRESHOLD + 10);
        assertThat(warnings).hasSize(1);
        assertThat(warnings.get(0)).startsWith("Guardrail scanned_tombstones violated: Scanned over")
                                   .endsWith("more than the warning threshold " + WARN_THRESHOLD);
    }

    @Test
    public void testViewReadOverFailThreshold()
    {
        createTableWithDeletedRows(FAIL_THRESHOLD + 10);

        ClientWarn.instance.captureWarnings();
        try
        {
            assertThatThrownBy(() -> execute(userClientState, "DELETE FROM %s WHERE k = 0 AND c >= 0 AND c < " + (FAIL_THRESHOLD + 10)))
            .isInstanceOfSatisfying(WriteFailureException.class, e ->
                assertThat(e.failureReasonByEndpoint).containsEntry(FBUtilities.getBroadcastAddressAndPort(),
                                                                    RequestFailureReason.READ_TOO_MANY_TOMBSTONES));
            List<String> warnings = ClientWarn.instance.getWarnings();
            assertThat(warnings).hasSize(1);
            assertThat(warnings.get(0)).startsWith("Guardrail scanned_tombstones violated: Scanned over")
                                       .endsWith("more than the maximum allowed " + FAIL_THRESHOLD + "; query aborted");
        }
        finally
        {
            ClientWarn.instance.resetWarnings();
        }
    }

    @Test
    public void testViewReadUnderWarnThreshold()
    {
        createTableWithDeletedRows(WARN_THRESHOLD - 10);

        assertThat(rangeDeletionWarnings(WARN_THRESHOLD - 10)).isNull();
    }

    private void createTableWithDeletedRows(int rows)
    {
        createTable("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY (k, c))");
        createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                   "WHERE k IS NOT NULL AND c IS NOT NULL AND v IS NOT NULL PRIMARY KEY (v, k, c)");
        for (int c = 0; c < rows; c++)
            executeInternal("DELETE FROM %s WHERE k = 0 AND c = ?", c);
    }

    // the range deletion reads the deleted rows of the base table to build the view updates
    private List<String> rangeDeletionWarnings(int rows)
    {
        ClientWarn.instance.captureWarnings();
        try
        {
            execute(userClientState, "DELETE FROM %s WHERE k = 0 AND c >= 0 AND c < " + rows);
            return ClientWarn.instance.getWarnings();
        }
        finally
        {
            ClientWarn.instance.resetWarnings();
        }
    }
}
