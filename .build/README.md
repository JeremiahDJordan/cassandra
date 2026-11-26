<!--
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
-->

Building and Testing with the helper sripts
-------------------------------------------

Information on building and testing beyond the use of ant.
All scripts also print help if the first argument is `-h`.

`build-jars.sh`, `check-code.sh` and `run-tests.sh` accept `-s` (`--summary`).
That prints a summary of the failures in place of the full ant output, and keeps
the full output in a log file under the build directory.  The scripts name that
file when a run fails.

Building JARs
-------------

Build in docker:

    .build/docker/build-jars.sh


Build without docker:

    .build/build-jars.sh


Remove the artifacts of a previous build first:

    .build/build-jars.sh --clean


Code Checks and Lints
---------------------

Run in docker:

    .build/docker/check-code.sh


Run without docker:

    .build/check-code.sh


Run in docker with a specific jdk.
The following applies to all build scripts.

    .build/docker/check-code.sh 11


Run in docker with a specific build path.
This permits parallel builds off the same source path.
The following applies to all build scripts.

    build_dir=/tmp/cass_Mtu462n .build/docker/check-code.sh


Building Artifacts (tarball and maven)
-------------------------------------

Build with docker:

    .build/docker/build-artifacts.sh


Build without docker:

    .build/build-artifacts.sh


Build in docker with a specific jdk:

    .build/docker/build-artifacts.sh 11


Building Debian and RedHat packages
-----------------------------------

The packaging scripts are only intended to be used with docker.

Build:

    .build/docker/build-debian.sh
    .build/docker/build-redhat.sh


Build with a specific jdk:

    .build/docker/build-debian.sh 11
    .build/docker/build-redhat.sh rpm 11


Build with legacy noboolean and a specific jdk:

    .build/docker/build-redhat.sh noboolean 11


Running Tests
-------------

Running unit tests with docker:

    .build/docker/run-tests.sh -a test


Running unittests without docker:

    .build/run-tests.sh -a test


Running only a split of unittests, with docker:

    .build/docker/run-tests.sh -a test -c 1/64


Running unittests with a specific jdk with docker:

    .build/docker/run-tests.sh -a test -c 1/64 -j 11


Running only unit tests matching a regexp, with docker:

    .build/docker/run-tests.sh -a test -t VerifyTest -j 11
    .build/docker/run-tests.sh -a test -t "Compaction*Test$" -j 11


Running other types of tests with docker:

    .build/docker/run-tests.sh -a test
    .build/docker/run-tests.sh -a stress-test
    .build/docker/run-tests.sh -a fqltool-test
    .build/docker/run-tests.sh -a test-cdc
    .build/docker/run-tests.sh -a test-compression
    .build/docker/run-tests.sh -a test-bigtable
    .build/docker/run-tests.sh -a test-system-keyspace-directory
    .build/docker/run-tests.sh -a test-burn
    .build/docker/run-tests.sh -a long-test
    .build/docker/run-tests.sh -a cqlsh-test
    .build/docker/run-tests.sh -a jvm-dtest
    .build/docker/run-tests.sh -a dtest
    .build/docker/run-tests.sh -a dtest-novnode
    .build/docker/run-tests.sh -a dtest-large
    .build/docker/run-tests.sh -a dtest-large-novnode
    .build/docker/run-tests.sh -a dtest-upgrade
    .build/docker/run-tests.sh -a dtest-upgrade-large

Repeating tests

Just add the '-repeat' suffix to the test type and pass in the repeat arguments

    .build/run-tests.sh -a jvm-dtest-repeat -t BooleanTest -e REPEATED_TESTS_COUNT=2
    .build/docker/run-tests.sh -a jvm-dtest-repeat -t BooleanTest -e REPEATED_TESTS_COUNT=2 -j 11

Fail fast with repeating tests is done with REPEATED_TESTS_STOP_ON_FAILURE

    .build/run-tests.sh -a jvm-dtest-repeat -t BooleanTest -e REPEATED_TESTS_COUNT=2 -e REPEATED_TESTS_STOP_ON_FAILURE=false

Running python dtests without docker:

    .build/run-python-dtests.sh dtest


Other test types without docker:

    .build/run-tests.sh jvm-test


Other python dtest types without docker:

    .build/run-python-dtests.sh dtest-upgrade-large

Debugging test scripts:

    DEBUG=true .build/docker/run-tests.sh -a test

