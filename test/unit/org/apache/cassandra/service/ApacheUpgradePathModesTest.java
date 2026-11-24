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

package org.apache.cassandra.service;

import org.junit.Test;

import org.apache.cassandra.service.ApacheUpgradePath.Modes;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ApacheUpgradePathModesTest
{
    @Test
    public void testParse()
    {
        Modes modes = Modes.parse("mode=UPGRADING;compat=CASSANDRA_5");
        assertEquals("UPGRADING", modes.mode);
        assertEquals("CASSANDRA_5", modes.compat);
        assertEquals("mode=UPGRADING;compat=CASSANDRA_5", modes.value());

        assertEquals("none", Modes.parse("future=1;compat=none;mode=NONE").compat);
        assertNull(Modes.parse(null));
    }
}
