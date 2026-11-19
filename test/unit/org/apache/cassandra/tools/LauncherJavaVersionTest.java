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

package org.apache.cassandra.tools;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Sources the launcher includes with fake {@code java} executables and checks the JVM options they assemble. Java 24
 * and later refuse to start with {@code -Djava.security.manager=allow}, which an operator may add to the options files.
 */
public class LauncherJavaVersionTest
{
    private static final String SM_ALLOW = "-Djava.security.manager=allow";
    private static final String JVM11_ONLY = "-XX:-UseBiasedLocking";
    private static final String UNSUPPORTED = "Cassandra 5.0 requires Java 11 or Java 17";
    private static final List<String> INCLUDES = Arrays.asList("bin/cassandra.in.sh", "tools/bin/cassandra.in.sh",
                                                               "redhat/cassandra.in.sh");

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private Path dir;
    private Path conf;

    @Before
    public void setUp() throws IOException
    {
        dir = folder.getRoot().toPath();
        conf = Files.createDirectories(dir.resolve("conf"));
        try (DirectoryStream<Path> files = Files.newDirectoryStream(Path.of("conf"), "jvm*.options"))
        {
            for (Path file : files)
                Files.copy(file, conf.resolve(file.getFileName().toString()));
        }
        Files.write(conf.resolve("jvm-server.options"), (SM_ALLOW + '\n').getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);
    }

    @Test
    public void testVersionComesFromTheSelectedJava() throws Exception
    {
        for (String include : INCLUDES)
        {
            // JAVA_HOME selects Java 25, PATH has Java 17
            Result mixed = source(include, "25.0.3", "17.0.19");
            assertEquals(include, "25", mixed.javaVersion);
            assertFalse(include, mixed.jvmOpts.contains(SM_ALLOW));
            assertFalse(include, mixed.jvmOpts.contains(JVM11_ONLY));

            Result reverse = source(include, "17.0.19", "25.0.3");
            assertEquals(include, "17", reverse.javaVersion);
            assertTrue(include, reverse.jvmOpts.contains(SM_ALLOW));

            Result jdk11 = source(include, "11.0.31", null);
            assertEquals(include, "11", jdk11.javaVersion);
            assertTrue(include, jdk11.jvmOpts.contains(SM_ALLOW));
            assertTrue(include, jdk11.jvmOpts.contains(JVM11_ONLY));
        }
    }

    @Test
    public void testEarlyAccessAndLegacyVersions() throws Exception
    {
        for (String include : INCLUDES)
        {
            for (String version : new String[]{ "25", "25-ea", "25-ea+14" })
            {
                Result result = source(include, version, null);
                assertEquals(include + ' ' + version, "25", result.javaVersion);
                assertFalse(include + ' ' + version, result.jvmOpts.contains(SM_ALLOW));
            }

            // unsupported Java versions are rejected; the tools include only warns
            for (String version : new String[]{ "1.8.0_462", "16.0.2" })
            {
                Result legacy = source(include, version, null);
                assertTrue(legacy.output, legacy.output.contains(UNSUPPORTED));
                if (!include.startsWith("tools/"))
                    assertEquals(include + ' ' + version, 1, legacy.exitCode);
            }

            Result garbage = sourceBanner(include, "no version here");
            assertEquals(include, 1, garbage.exitCode);
            assertTrue(garbage.output, garbage.output.contains("Unable to determine the Java version of"));
        }
    }

    @Test
    public void testToolOptionsPreamble() throws Exception
    {
        for (String include : INCLUDES)
        {
            Result result = sourceBanner(include,
                                         "Picked up JAVA_TOOL_OPTIONS: -Dexample.label=\"17 java version\"",
                                         "openjdk version \"25.0.5\" 2025-10-21 LTS");
            assertEquals(include + ' ' + result.output, "25", result.javaVersion);
            assertFalse(include, result.jvmOpts.contains(SM_ALLOW));

            Result unparsable = sourceBanner(include,
                                             "Picked up JAVA_TOOL_OPTIONS: -Dexample.label=\"java version 17\"",
                                             "openjdk version \"17.0.19\" 2026-04-21 LTS");
            assertEquals(include + ' ' + unparsable.output, "17", unparsable.javaVersion);
            assertTrue(include, unparsable.jvmOpts.contains(SM_ALLOW));
        }
    }

    private Result source(String include, String javaHomeVersion, String pathVersion) throws Exception
    {
        Path javaHome = fakeJdk("home-" + javaHomeVersion, "openjdk version \"" + javaHomeVersion + "\" 2026-01-20");
        Path pathJdk = pathVersion == null ? null : fakeJdk("path-" + pathVersion, "openjdk version \"" + pathVersion + "\" 2026-01-20");
        return run(include, javaHome, pathJdk);
    }

    private Result sourceBanner(String include, String... banner) throws Exception
    {
        return run(include, fakeJdk("banner", banner), null);
    }

    private Result run(String include, Path javaHome, Path pathJdk) throws Exception
    {
        Path script = Path.of(include).toAbsolutePath();
        if (include.startsWith("redhat/"))
        {
            // the Red Hat include hardcodes its installed configuration directory
            String text = new String(Files.readAllBytes(script), StandardCharsets.UTF_8)
                          .replace("CASSANDRA_CONF=/etc/cassandra/conf", "CASSANDRA_CONF=" + conf)
                          .replace("CASSANDRA_HOME=/usr/share/cassandra", "CASSANDRA_HOME=" + dir.resolve("home"));
            script = dir.resolve("redhat-cassandra.in.sh");
            Files.write(script, text.getBytes(StandardCharsets.UTF_8));
        }
        Files.createDirectories(dir.resolve("home"));

        // every variable the includes read, so that the inherited environment does not matter
        Map<String, String> env = new HashMap<>();
        env.put("HOME", dir.toString());
        env.put("PATH", (pathJdk == null ? "" : pathJdk.resolve("bin") + ":") + "/usr/bin:/bin");
        env.put("JAVA_HOME", javaHome.toString());
        env.put("CASSANDRA_HOME", dir.resolve("home").toString());
        env.put("CASSANDRA_CONF", conf.toString());
        env.put("jvmoptions_variant", "-server");
        for (String unset : new String[]{ "CLASSPATH", "EXTRA_CLASSPATH", "JAVA", "JAVA_AGENT", "JAVA_OPTS", "JVM_OPTS" })
            env.put(unset, "");
        // empty stdin: the tools include does not exit on an unsupported Java version, and then greps an unset file name
        try (ToolRunner.ObservableTool tool = ToolRunner.invokeAsync(env, new ByteArrayInputStream(new byte[0]),
                                                                     Arrays.asList("bash", "-c",
                                                                                   ". \"" + script + "\"; printf 'VERSION=%s\nOPTS=%s\n' \"$JAVA_VERSION\" \"$JVM_OPTS\"")))
        {
            await().atMost(60, TimeUnit.SECONDS).until(tool::isDone);
            ToolRunner.ToolResult result = tool.waitComplete();
            return new Result(result.getExitCode(), result.getStdout() + result.getStderr());
        }
    }

    private Path fakeJdk(String name, String... banner) throws IOException
    {
        Path home = dir.resolve(name.replaceAll("[^A-Za-z0-9.-]", "_"));
        Path bin = Files.createDirectories(home.resolve("bin"));
        Path java = bin.resolve("java");
        StringBuilder script = new StringBuilder("#!/bin/sh\n");
        for (String line : banner)
            script.append("echo '").append(line).append("' >&2\n");
        Files.write(java, script.toString().getBytes(StandardCharsets.UTF_8));
        Files.setPosixFilePermissions(java, PosixFilePermissions.fromString("rwxr-xr-x"));
        return home;
    }

    private static class Result
    {
        final int exitCode;
        final String output;
        final String javaVersion;
        final String jvmOpts;

        Result(int exitCode, String output)
        {
            this.exitCode = exitCode;
            this.output = output;
            this.javaVersion = value(output, "VERSION=");
            this.jvmOpts = value(output, "OPTS=");
        }

        private static String value(String output, String prefix)
        {
            for (String line : output.split("\\n"))
                if (line.startsWith(prefix))
                    return line.substring(prefix.length());
            return "";
        }
    }
}
