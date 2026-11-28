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

package org.apache.cassandra;

import java.io.ByteArrayOutputStream;

import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.tools.ant.taskdefs.optional.junit.JUnitTest;
import org.junit.Before;
import org.junit.Test;
import org.w3c.dom.Document;

import static org.apache.cassandra.CassandraXMLJUnitResultFormatter.OUTPUT_HEAD_CHARS;
import static org.apache.cassandra.CassandraXMLJUnitResultFormatter.OUTPUT_TAIL_CHARS;
import static org.junit.Assert.assertEquals;

public class CassandraXMLJUnitResultFormatterTest
{
    private static final DocumentBuilderFactory DOCUMENT_BUILDER_FACTORY = DocumentBuilderFactory.newInstance();

    private CassandraXMLJUnitResultFormatter formatter;
    private ByteArrayOutputStream out;

    @Before
    public void setUp()
    {
        formatter = new CassandraXMLJUnitResultFormatter();
        out = new ByteArrayOutputStream();
        formatter.setOutput(out);
        JUnitTest suite = new JUnitTest("org.apache.cassandra.SomeTest");
        formatter.startTestSuite(suite);
    }

    @Test
    public void testLongOutputKeepsHeadAndTail() throws Exception
    {
        String head = StringUtils.repeat('h', OUTPUT_HEAD_CHARS);
        String tail = StringUtils.repeat('t', OUTPUT_TAIL_CHARS);
        formatter.setSystemOutput(head + StringUtils.repeat('o', 1000) + tail);
        formatter.setSystemError(head + StringUtils.repeat('e', 10) + tail);

        JUnitTest suite = new JUnitTest("org.apache.cassandra.SomeTest");
        suite.setCounts(0, 0, 0, 0);
        formatter.endTestSuite(suite);

        Document doc = parseOutput();
        assertEquals(head + "\n--- 1000 characters of output omitted, logger output is in the test's log files under build/test/logs ---\n" + tail,
                     doc.getElementsByTagName("system-out").item(0).getTextContent());
        assertEquals(head + "\n--- 10 characters of output omitted, logger output is in the test's log files under build/test/logs ---\n" + tail,
                     doc.getElementsByTagName("system-err").item(0).getTextContent());
    }

    @Test
    public void testOutputUpToLimitIsUnchanged() throws Exception
    {
        String output = StringUtils.repeat('o', OUTPUT_HEAD_CHARS + OUTPUT_TAIL_CHARS);
        formatter.setSystemOutput(output);

        JUnitTest suite = new JUnitTest("org.apache.cassandra.SomeTest");
        suite.setCounts(0, 0, 0, 0);
        formatter.endTestSuite(suite);

        assertEquals(output, parseOutput().getElementsByTagName("system-out").item(0).getTextContent());
    }

    private Document parseOutput() throws Exception
    {
        return DOCUMENT_BUILDER_FACTORY.newDocumentBuilder().parse(new java.io.ByteArrayInputStream(out.toByteArray()));
    }
}
