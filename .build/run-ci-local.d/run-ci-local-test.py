#!/usr/bin/env python3
# -*- coding: utf-8 -*-
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
# Used to test `.build/run-ci-local`
# Run with `python3 .build/run-ci-local.d/run-ci-local-test.py`
#
# TestJenkinsfileCells runs the Jenkinsfile's tasks() in Groovy, with `groovy` on the PATH or
# GROOVY_JAR=<groovy-4.x.jar>, and is skipped without either.


import contextlib
import fcntl
import io
import json
import lzma
import os
import re
import shutil
import stat
import subprocess
import tarfile
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

import run_ci_local

from run_ci_local import (
    CASSANDRA_DIR,
    Cell,
    HostError,
    OwnerError,
    JenkinsfileError,
    build_properties,
    build_xml_property,
    cell_times,
    check_name,
    jenkinsfile_drift,
    load_config,
    load_hosts,
    lock_label,
    parse_groovy_literal,
    plan_cells,
    read_pipeline,
    summarise,
    write_source_tarball,
)

JENKINSFILE = CASSANDRA_DIR / ".jenkins" / "Jenkinsfile"
JOB_SH = CASSANDRA_DIR / ".build" / "run-ci-local.d" / "remote" / "job.sh"
STRESS = "stress-test_jdk11_python_3.8_no_amd64_1_1"
# owner markers, as the driver writes them
OWNER, ONE, TWO = "0123456789abcdef", "1111111111111111", "2222222222222222"
# a run of a driver
SESSION = "5e55105e55105e55"


BUILD_XML = (CASSANDRA_DIR / "build.xml").read_text()


def in_tree_cells(profile, jdk="", profile_custom_regexp="", test_regexp="", architecture="amd64"):
    pipeline = read_pipeline(JENKINSFILE.read_text())
    return plan_cells(pipeline, build_xml_property(BUILD_XML, "java.default"),
                      build_xml_property(BUILD_XML, "java.supported").split(","), profile,
                      profile_custom_regexp, jdk, test_regexp, architecture)


def fake_runner(label, cell_id):
    """A process job.sh takes for the runner of the cell, leading its own session"""
    return subprocess.Popen(["setsid", "bash", "-c", f'exec -a "job.sh _run {label} {cell_id} x" sleep 60'])


def write_executable(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    path.chmod(path.stat().st_mode | stat.S_IXUSR)


class TestGroovyLiteral(unittest.TestCase):

    def test_map_of_lists(self):
        text = """[
            'a': ['x', 'y'], // comment
            /* comment */ 'b': [],
            'c w/ space': ["z"],
        ]"""
        self.assertEqual({'a': ['x', 'y'], 'b': [], 'c w/ space': ['z']}, parse_groovy_literal(text))

    def test_bare_keys_and_scalars(self):
        self.assertEqual({'splits': 4, 'size': 'large', 'benchmark': True, 'toCopy': None},
                         parse_groovy_literal("[splits: 4, size: 'large', benchmark: true, toCopy: null]"))

    def test_nested_and_empty(self):
        self.assertEqual({'jar': {'script': 'build-jars.sh'}, 'custom': {}, 'x': [[1], []]},
                         parse_groovy_literal("['jar': [script: 'build-jars.sh'], 'custom': [:], 'x': [[1], []]]"))

    def test_starts_at_position(self):
        text = "def x = ['a', 'b'] + other"
        self.assertEqual(['a', 'b'], parse_groovy_literal(text, text.index('[')))

    def test_escapes(self):
        self.assertEqual(["it's", 'a\\b'], parse_groovy_literal("['it\\'s', 'a\\\\b']"))

    def test_rejects_non_literals(self):
        for text in ("[splits: max]", "['a', b]", '["${x}"]', "['a' + 'b']", "['a'"):
            with self.subTest(text=text), self.assertRaises(JenkinsfileError):
                parse_groovy_literal(text)


class TestJenkinsfile(unittest.TestCase):

    def test_in_tree_jenkinsfile_matches_what_run_ci_local_reproduces(self):
        # On failure: tasks(), build(), test() or generateTestReports() changed in .jenkins/Jenkinsfile.  Make the same
        # change in .build/run-ci-local (and remote/job.sh), then update JENKINSFILE_MIRRORED.
        self.assertEqual([], jenkinsfile_drift(JENKINSFILE.read_text()))

    def test_drift_is_reported(self):
        text = JENKINSFILE.read_text()
        changed = text.replace("axis['python'] == '3.13'", "axis['python'] == '3.14'")
        self.assertNotEqual(text, changed)
        drift = jenkinsfile_drift(changed)
        self.assertEqual(1, len(drift))
        self.assertEqual([("tasks", None)], drift)

    def test_drift_is_checked_in_the_function_it_belongs_to(self):
        # build() and test() both have the line; only test() changes
        text = JENKINSFILE.read_text()
        test_start = text.index("def test(command, cell)")
        changed = text[:test_start] + text[test_start:].replace("def maxAttempts = 2", "def maxAttempts = 3", 1)
        self.assertEqual([("test", "def maxAttempts = 2")], jenkinsfile_drift(changed))

    def test_an_inserted_filter_is_drift(self):
        anchor = "def List _axes = getMatrixAxes(matrix_axes).findAll { axis ->\n"
        text = JENKINSFILE.read_text()
        self.assertIn(anchor, text)
        changed = text.replace(anchor, anchor + "    (axis['jdk'] != '17') &&\n")
        self.assertEqual([("tasks", None)], jenkinsfile_drift(changed))

    def test_new_splits_and_comments_are_not_drift(self):
        text = JENKINSFILE.read_text()
        changed = text.replace("'test': [splits: 20],", "'test': [splits: 24], // more").replace(
            "def javaVersionPythonDtests", "// a comment\n  def   javaVersionPythonDtests")
        self.assertNotEqual(text, changed)
        self.assertEqual([], jenkinsfile_drift(changed))

    def test_a_string_change_is_drift(self):
        text = JENKINSFILE.read_text()
        changed = text.replace('def cython = "no" == cell.cython ? "" : " cython"',
                               'def cython = "no" == cell.cython ? "" : "  cython"')
        self.assertEqual([("getStepName", None)], jenkinsfile_drift(changed))

    def test_comment_and_whitespace_changes_are_not_drift(self):
        text = JENKINSFILE.read_text().replace("def maxAttempts = 2", "def   maxAttempts =  2 // retries")
        self.assertEqual([], jenkinsfile_drift(text))

    def test_in_tree_pipeline(self):
        pipeline = read_pipeline(JENKINSFILE.read_text())
        self.assertEqual({'small': 1, 'medium': 3, 'large': 7, 'report': 2}, pipeline['sizes'])
        for profile in ('packaging', 'skinny', 'pre-commit', 'post-commit', 'custom'):
            self.assertIn(profile, pipeline['profiles'])
        steps = pipeline['steps']
        self.assertEqual({'script': 'build-jars.sh', 'toCopy': None, 'type': 'build', 'size': 'small', 'splits': 1,
                          'timeout_hours': 1}, steps['jar'])
        self.assertEqual('medium', steps['lint']['size'])
        self.assertEqual('build-redhat.sh rpm', steps['redhat']['script'])
        self.assertEqual({'splits': 20, 'type': 'test', 'size': 'medium', 'timeout_hours': 1}, steps['test'])
        self.assertEqual({'splits': 64, 'size': 'large', 'type': 'test', 'timeout_hours': 1, 'python-dtest': True},
                         steps['dtest'])
        self.assertEqual(6, steps['microbench']['timeout_hours'])
        self.assertTrue(steps['microbench']['benchmark'])
        self.assertEqual(['3.8', '3.11', '3.12', '3.13'], pipeline['pythons'])
        self.assertEqual(['amd64', 'arm64'], pipeline['archs'])
        self.assertEqual({'repository': '', 'branch': '', 'profile': 'skinny', 'profile_custom_regexp': '',
                          'architecture': 'amd64', 'jdk': '',
                          'dtest_repository': 'https://github.com/apache/cassandra-dtest', 'dtest_branch': 'trunk'},
                         pipeline['defaults'])
        self.assertEqual('3.8', pipeline['python_default'])
        # every step a profile names is defined
        for profile, names in pipeline['profiles'].items():
            for name in names:
                self.assertIn(name, steps, f"{profile}: {name}")

    def test_non_literal_steps_are_rejected(self):
        text = JENKINSFILE.read_text().replace("'test': [splits: 20],", "'test': [splits: testSplits],")
        with self.assertRaises(JenkinsfileError):
            read_pipeline(text)

    def test_a_new_jenkins_parameter_is_rejected(self):
        text = JENKINSFILE.read_text().replace("    string(name: 'dtest_branch',",
                                               "    string(name: 'ccm_branch', defaultValue: ''),\n"
                                               "    string(name: 'dtest_branch',")
        with self.assertRaisesRegex(JenkinsfileError, "ccm_branch"):
            read_pipeline(text)

    def test_missing_definition_is_rejected(self):
        text = JENKINSFILE.read_text().replace("def testSteps = [", "def unitSteps = [")
        with self.assertRaisesRegex(JenkinsfileError, "testSteps not found"):
            read_pipeline(text)


class TestPlanCells(unittest.TestCase):
    PIPELINE = {
        'profiles': {'p': ['lint', 'cqlsh-test', 'test', 'simulator-dtest', 'dtest', 'dtest-upgrade'], 'custom': []},
        'steps': {
            'jar': {'type': 'build', 'size': 'small', 'splits': 1, 'timeout_hours': 1, 'script': 'build-jars.sh'},
            'lint': {'type': 'build', 'size': 'medium', 'splits': 1, 'timeout_hours': 1, 'script': 'check-code.sh'},
            'cqlsh-test': {'type': 'test', 'size': 'medium', 'splits': 1, 'timeout_hours': 1},
            'test': {'type': 'test', 'size': 'medium', 'splits': 3, 'timeout_hours': 1},
            'simulator-dtest': {'type': 'test', 'size': 'large', 'splits': 2, 'timeout_hours': 1},
            'dtest': {'type': 'test', 'size': 'large', 'splits': 2, 'timeout_hours': 1, 'python-dtest': True},
            'dtest-upgrade': {'type': 'test', 'size': 'large', 'splits': 1, 'timeout_hours': 2, 'python-dtest': True},
        },
        'pythons': ['3.8', '3.12'],
        'python_default': '3.8',
        'sizes': {'small': 1, 'medium': 3, 'large': 7},
        'archs': ['amd64', 'arm64'],
    }

    def names(self, profile='p', jdk='', regexp='', test=''):
        return [c.name for c in plan_cells(self.PIPELINE, '11', ['11', '17'], profile, regexp, jdk, test)]

    def test_axes(self):
        self.assertEqual(['jar jdk11', 'jar jdk17',
                          'cqlsh-test jdk11 python3.8 cython', 'lint jdk11', 'cqlsh-test jdk11 python3.8',
                          'test jdk11 1/3', 'test jdk11 2/3', 'test jdk11 3/3',
                          'simulator-dtest jdk11 1/2', 'simulator-dtest jdk11 2/2',
                          'dtest jdk11 1/2', 'dtest jdk11 2/2', 'dtest-upgrade jdk11', 'cqlsh-test jdk11 python3.12',
                          'lint jdk17', 'test jdk17 1/3', 'test jdk17 2/3', 'test jdk17 3/3'],
                         self.names())

    def test_jdk_moves_python_dtests(self):
        # python dtests run on the requested jdk; cqlsh-test, simulator-dtest and *dtest-upgrade on the default only
        self.assertEqual(['jar jdk17', 'lint jdk17', 'test jdk17 1/3', 'test jdk17 2/3', 'test jdk17 3/3',
                          'dtest jdk17 1/2', 'dtest jdk17 2/2'], self.names(jdk='17'))

    def test_custom_profile_is_a_full_match(self):
        self.assertEqual(['jar jdk11', 'jar jdk17', 'dtest jdk11 1/2', 'dtest jdk11 2/2'],
                         self.names('custom', regexp='dtest'))
        self.assertEqual(['jar jdk11', 'jar jdk17', 'dtest jdk11 1/2', 'dtest jdk11 2/2', 'dtest-upgrade jdk11'],
                         self.names('custom', regexp='dtest.*'))

    def test_test_regexp_replaces_splits(self):
        cells = plan_cells(self.PIPELINE, '11', ['11', '17'], 'custom', 'test|lint', '11', 'FooTest')
        self.assertEqual(['jar jdk11', 'lint jdk11', 'test jdk11 -t FooTest'], [c.name for c in cells])
        self.assertEqual('FooTest', cells[2].test_regexp)

    def test_architectures(self):
        # other architectures build only, as in the Jenkinsfile
        cells = plan_cells(self.PIPELINE, '11', ['11', '17'], 'p', jdk='11', architecture='arm64')
        self.assertEqual(['jar arm64 jdk11', 'lint arm64 jdk11'], [c.name for c in cells])
        self.assertEqual(('arm64', '_jdk11_arm64', 'base-arm64-jdk11.tar'),
                         (cells[1].arch, cells[1].suffix, cells[1].base))
        both = [c.name for c in plan_cells(self.PIPELINE, '11', ['11', '17'], 'p', jdk='11', architecture='all')]
        self.assertIn('jar arm64 jdk11', both)
        self.assertIn('test jdk11 1/3', both)
        self.assertNotIn('test arm64 jdk11 1/3', both)
        with self.assertRaisesRegex(ValueError, "architecture 'riscv64'"):
            plan_cells(self.PIPELINE, '11', ['11'], 'p', architecture='riscv64')

    def test_errors(self):
        with self.assertRaisesRegex(ValueError, "no profile"):
            self.names('nope')
        with self.assertRaisesRegex(ValueError, "needs a regexp"):
            self.names('custom')
        with self.assertRaisesRegex(ValueError, "not in build.xml"):
            self.names(jdk='8')

    def test_cell_identity(self):
        cells = {c.name: c for c in plan_cells(self.PIPELINE, '11', ['11', '17'], 'p')}
        test = cells['test jdk17 2/3']
        self.assertEqual(('test', 3, 'medium'), (test.kind, test.units, test.command['size']))
        self.assertEqual('_jdk17_python_3.8_no_amd64_2_3', test.suffix)
        self.assertEqual('test_jdk17_python_3.8_no_amd64_2_3', test.cell_id)
        cqlsh = cells['cqlsh-test jdk11 python3.8 cython']
        self.assertEqual('_jdk11_python_3.8_yes_amd64_1_1', cqlsh.suffix)
        lint = cells['lint jdk11']
        self.assertEqual(('build', 'lint_jdk11_amd64'), (lint.kind, lint.cell_id))
        self.assertEqual('jar', cells['jar jdk11'].kind)

    def test_env(self):
        cell = Cell('dtest-upgrade', self.PIPELINE['steps']['dtest-upgrade'], 7, '11', '3.8', 'no')
        env = dict(line.split('=', 1) for line in cell.env(3, 'podman').splitlines())
        self.assertEqual('6', env['TIMEOUT_HOURS'])
        self.assertEqual('yes', env['PYTHON_DTEST'])
        self.assertEqual('podman', env['RUNTIME'])
        self.assertEqual("''", env['TEST_REGEXP'])
        lint = Cell('redhat', {'type': 'build', 'splits': 1, 'timeout_hours': 1, 'script': 'build-redhat.sh rpm'},
                    1, '11', '3.8', 'no')
        self.assertIn("SCRIPT='build-redhat.sh rpm'", lint.env(1, None))

    def test_in_tree_profiles(self):
        names = [c.name for c in in_tree_cells('skinny', jdk='11')]
        self.assertIn('jvm-dtest jdk11 16/16', names)
        self.assertIn('cqlsh-test jdk11 python3.13', names)
        self.assertNotIn('cqlsh-test jdk11 python3.13 cython', names)
        self.assertIn('dtest jdk11 64/64', names)


def groovy_command():
    if shutil.which("groovy"):
        return ["groovy"]
    if os.environ.get("GROOVY_JAR"):
        return ["java", "-cp", os.environ["GROOVY_JAR"], "groovy.ui.GroovyMain"]
    return None


@unittest.skipIf(groovy_command() is None, "needs groovy on the PATH, or GROOVY_JAR")
class TestJenkinsfileCells(unittest.TestCase):
    """The cells of every profile and jdk equal those the Jenkinsfile's own tasks() creates"""

    def jenkins_cells(self, profile, jdk, regexp, architecture):
        script = CASSANDRA_DIR / ".build/run-ci-local.d/jenkinsfile-cells.groovy"
        output = subprocess.run(groovy_command() + [str(script), str(CASSANDRA_DIR), profile, jdk, architecture,
                                                    regexp],
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=False)
        self.assertEqual(0, output.returncode, output.stderr)
        return sorted(output.stdout.split("\n")[:-1])

    def test_every_profile_and_jdk(self):
        pipeline = read_pipeline(JENKINSFILE.read_text())
        jdks = [''] + build_xml_property(BUILD_XML, "java.supported").split(",")
        cases = [(p, j, '') for p in pipeline['profiles'] if p != 'custom' for j in jdks]
        cases = [(p, j, '', 'amd64') for p, j, _ in cases]
        cases += [('custom', '', r, 'amd64')
                  for r in ('stress.*|jvm-dtest.*', 'dtest', 'cqlsh-test|lint', 'dtest-upgrade.*')]
        cases += [(p, j, '', a) for p in ('packaging', 'pre-commit') for j in ('', '17') for a in ('arm64', 'all')]
        for profile, jdk, regexp, architecture in cases:
            with self.subTest(profile=profile, jdk=jdk, regexp=regexp, architecture=architecture):
                self.assertEqual(self.jenkins_cells(profile, jdk, regexp, architecture),
                                 sorted(c.name for c in in_tree_cells(profile, jdk, regexp, architecture=architecture)))


class TestHosts(unittest.TestCase):

    def test_hosts_file(self):
        with tempfile.NamedTemporaryFile("w", suffix=".hosts") as f:
            f.write("# comment\nalpha 32\n\nbeta runtime=podman arch=arm64  # shared\nlocalhost 8 runtime=docker\n")
            f.flush()
            hosts = load_hosts(f.name, '.run-ci-local')
        self.assertEqual([('alpha', 32, None, None), ('beta', None, 'podman', 'arm64'),
                          ('localhost', 8, 'docker', None)], [(h.name, h.capacity, h.runtime, h.arch) for h in hosts])
        self.assertTrue(hosts[2].local)
        self.assertEqual(['bash', '-c', 'true'], hosts[2].command('true'))
        command = hosts[0].command("echo 'a b'")
        self.assertIn('BatchMode=yes', command)
        # bash runs the command, whatever the login shell; ssh options end before the host
        self.assertEqual(['--', 'alpha', "exec bash -c 'echo '\"'\"'a b'\"'\"''"], command[-3:])
        control = [o for o in command if o.startswith("ControlPath=")][0][len("ControlPath="):]
        self.assertEqual(0o700, stat.S_IMODE(os.stat(os.path.dirname(control)).st_mode))

    def test_bad_hosts_file(self):
        for text in ("alpha cpus=3\n", "# nothing\n", "alpha runtime=lxc\n", "-oProxyCommand=x 8\n"):
            with self.subTest(text=text), tempfile.NamedTemporaryFile("w") as f:
                f.write(text)
                f.flush()
                with self.assertRaises(ValueError):
                    load_hosts(f.name, '.run-ci-local')

    def test_example_hosts_file(self):
        hosts = load_hosts(CASSANDRA_DIR / ".build/run-ci-local.d/hosts.example", ".run-ci-local")
        self.assertTrue(hosts)
        self.assertEqual(['"$HOME"/.run-ci-local', '/scratch/run-ci-local'],
                         sorted({h.remote.home for h in hosts}))


class TestConfig(unittest.TestCase):

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.tmp)

    def config(self, text):
        path = self.tmp / "run-ci-local.conf"
        path.write_text(text)
        return load_config(path, required=True)

    def test_values_are_typed(self):
        self.assertEqual({"profile": "pre-commit w/ upgrades", "jdk": "17", "fail_fast": False, "timeout_factor": 2,
                          "report": True, "hosts": "~/hosts"},
                         self.config("[run-ci-local]\nprofile = pre-commit w/ upgrades\njdk = 17\nfail_fast = no\n"
                                     "timeout_factor = 2\nreport = yes\nhosts = ~/hosts\n"))

    def test_errors(self):
        for text, message in (("[run-ci-local]\nprofil = skinny\n", "unknown key"),
                              ("[jenkins]\nprofile = skinny\n", "only a \\[run-ci-local\\] section"),
                              ("[run-ci-local]\nfail_fast = maybe\n", "fail_fast"),
                              ("[run-ci-local]\ntimeout_factor = two\n", "timeout_factor")):
            with self.subTest(text=text), self.assertRaisesRegex(SystemExit, message):
                self.config(text)

    def test_absent_file(self):
        self.assertEqual({}, load_config(self.tmp / "absent.conf", required=False))
        with self.assertRaisesRegex(SystemExit, "does not exist"):
            load_config(self.tmp / "absent.conf", required=True)

    def test_example_file(self):
        example = (CASSANDRA_DIR / ".build/run-ci-local.d/run-ci-local.conf.example").read_text()
        self.assertEqual({}, self.config(example))
        # every key it documents is one the tool reads
        uncommented = re.sub(r"^#(\w+ =)", r"\1", example, flags=re.M)
        self.assertEqual(set(run_ci_local.CONFIG_KEYS), set(self.config(uncommented)))


class TestSource(unittest.TestCase):

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.tmp)

    def git(self, cwd, *args):
        subprocess.run(["git", "-C", str(cwd), "-c", "user.name=t", "-c", "user.email=t@example.com",
                        "-c", "protocol.file.allow=always", *args], check=True, stdout=subprocess.DEVNULL,
                       stderr=subprocess.DEVNULL)

    def test_tarball_has_submodules_git_and_properties(self):
        module = self.tmp / "module"
        module.mkdir()
        (module / "lib.txt").write_text("module file\n")
        self.git(module, "init", "-q")
        self.git(module, "add", ".")
        self.git(module, "commit", "-q", "-m", "module")
        src = self.tmp / "src"
        src.mkdir()
        (src / "build.xml").write_text("<project/>\n")
        self.git(src, "init", "-q", "-b", "feature")
        self.git(src, "submodule", "add", "-q", str(module), "modules/m")
        self.git(src, "add", ".")
        self.git(src, "commit", "-q", "-m", "src")
        (src / "build.properties").write_text("a=1\n")
        sha = subprocess.run(["git", "-C", str(src), "rev-parse", "HEAD"], stdout=subprocess.PIPE, text=True,
                             check=True).stdout.strip()
        tar_path = self.tmp / "src.tar"
        write_source_tarball(src, sha, tar_path, build_properties(src, "https://mirror.example/maven/"))
        extracted = self.tmp / "x"
        with tarfile.open(tar_path) as tar:
            tar.extractall(extracted, **run_ci_local.TAR_FILTER)
        self.assertEqual("module file\n", (extracted / "modules/m/lib.txt").read_text())
        properties = (extracted / "build.properties").read_text()
        self.assertTrue(properties.startswith("a=1\n"))
        self.assertIn("artifact.remoteRepository.central=https://mirror.example/maven\n", properties)
        self.assertIn("resolver-ant-tasks.url=https://mirror.example/maven/org/apache/maven/resolver/", properties)
        head = subprocess.run(["git", "-C", str(extracted), "rev-parse", "HEAD"], stdout=subprocess.PIPE, text=True,
                              check=True).stdout.strip()
        branch = subprocess.run(["git", "-C", str(extracted), "branch", "--show-current"], stdout=subprocess.PIPE,
                                text=True, check=True).stdout.strip()
        self.assertEqual((sha, "feature"), (head, branch))
        # the build's `git submodule update --init` finds the submodule checked out, and does not clone it
        module_sha = subprocess.run(["git", "-C", str(module), "rev-parse", "HEAD"], stdout=subprocess.PIPE, text=True,
                                    check=True).stdout.strip()
        shutil.rmtree(module)
        update = subprocess.run(["git", "-C", str(extracted), "submodule", "update", "--init", "--recursive"],
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, check=False)
        self.assertEqual(0, update.returncode, update.stdout)
        self.assertEqual("module file\n", (extracted / "modules/m/lib.txt").read_text())
        self.assertEqual(module_sha, subprocess.run(["git", "-C", str(extracted / "modules/m"), "rev-parse", "HEAD"],
                                                    stdout=subprocess.PIPE, text=True, check=True).stdout.strip())
        status = subprocess.run(["git", "-C", str(extracted), "status", "--porcelain"], stdout=subprocess.PIPE,
                                text=True, check=True)
        self.assertEqual("?? build.properties", status.stdout.strip())

    def test_tarball_names_the_origin_branch(self):
        src = self.tmp / "src"
        src.mkdir()
        (src / "build.xml").write_text("<project/>\n")
        self.git(src, "init", "-q", "-b", "local")
        self.git(src, "add", ".")
        self.git(src, "commit", "-q", "-m", "src")
        sha = subprocess.run(["git", "-C", str(src), "rev-parse", "HEAD"], stdout=subprocess.PIPE, text=True,
                             check=True).stdout.strip()
        write_source_tarball(src, sha, self.tmp / "src.tar", None, "trunk", "https://github.com/apache/cassandra")
        extracted = self.tmp / "x"
        with tarfile.open(self.tmp / "src.tar") as tar:
            tar.extractall(extracted, **run_ci_local.TAR_FILTER)

        def git_out(*args):
            return subprocess.run(["git", "-C", str(extracted), *args], stdout=subprocess.PIPE, text=True,
                                  check=True).stdout.strip()
        # what generate-ci-summary.sh reads
        self.assertEqual("https://github.com/apache/cassandra", git_out("remote", "get-url", "origin"))
        self.assertIn("origin/trunk", git_out("branch", "--remote", "--contains"))
        self.assertEqual(sha, git_out("rev-parse", "HEAD"))
        self.assertEqual("trunk", git_out("branch", "--show-current"))

    def test_no_properties(self):
        self.assertIsNone(build_properties(self.tmp, ""))

    def test_submodule_not_checked_out(self):
        module = self.tmp / "module"
        module.mkdir()
        (module / "f").write_text("x")
        self.git(module, "init", "-q")
        self.git(module, "add", ".")
        self.git(module, "commit", "-q", "-m", "m")
        src = self.tmp / "src"
        src.mkdir()
        self.git(src, "init", "-q")
        self.git(src, "submodule", "add", "-q", str(module), "modules/m")
        self.git(src, "commit", "-q", "-m", "src")
        self.git(src, "submodule", "deinit", "-q", "-f", "modules/m")
        sha = subprocess.run(["git", "-C", str(src), "rev-parse", "HEAD"], stdout=subprocess.PIPE, text=True,
                             check=True).stdout.strip()
        with self.assertRaisesRegex(SystemExit, "git submodule update --init modules/m"):
            write_source_tarball(src, sha, self.tmp / "src.tar", None)


JUNIT = """<?xml version="1.0" encoding="UTF-8"?>
<testsuite name="org.apache.cassandra.FooTest" tests="3">
  <testcase classname="org.apache.cassandra.FooTest" name="testA" time="1.5"/>
  <testcase classname="org.apache.cassandra.FooTest" name="testB" time="2.0"><failure message="boom"/></testcase>
  <testcase classname="org.apache.cassandra.BarTest" name="testC" time="0.5"><skipped/></testcase>
</testsuite>
"""


class TestResults(unittest.TestCase):

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.tmp)

    def test_cell_times(self):
        output = self.tmp / "output/test/jdk_11/amd64"
        output.mkdir(parents=True)
        with lzma.open(output / "TEST-org.apache.cassandra.FooTest.xml.xz", "wt") as f:
            f.write(JUNIT)
        cell = Cell('test', {'type': 'test', 'splits': 20, 'timeout_hours': 1}, 3, '11', '3.8', 'no', 7)
        tsv, suites = cell_times(cell, 123000, "ok", self.tmp / "output")
        self.assertEqual("test\tamd64\t11\t7\t20\t1\t123000\t4.000\tok\n", tsv)
        self.assertEqual("3.500\torg.apache.cassandra.FooTest\n0.500\torg.apache.cassandra.BarTest\n", suites)

    def test_summary(self):
        out = self.tmp / "label"
        attempt = out / "cells/test_jdk11_python_3.8_no_amd64_1_2/attempt2/output/test/jdk_11/amd64"
        attempt.mkdir(parents=True)
        (attempt / "TEST-org.apache.cassandra.FooTest.xml").write_text(JUNIT)
        # an earlier attempt's results are not counted
        old = out / "cells/test_jdk11_python_3.8_no_amd64_1_2/attempt1/output"
        old.mkdir(parents=True)
        (old / "TEST-x.xml").write_text(JUNIT)
        (out / "state.json").write_text(json.dumps({"src": "/src", "sha": "abc", "cells": {
            "test_jdk11_python_3.8_no_amd64_1_2": {"name": "test jdk11 1/2", "status": "done", "rc": 0, "attempt": 2,
                                                   "host": "alpha", "duration_s": 600, "kind": "test", "junit": 1},
            "test_jdk11_python_3.8_no_amd64_2_2": {"name": "test jdk11 2/2", "status": "not run", "rc": None,
                                                   "kind": "test"}}}))
        text, result = summarise(out)
        self.assertEqual("FAILURE", result)
        self.assertIn("result: FAILURE", text)
        self.assertIn("tests: passed 1 | failed 1 | skipped 1", text)
        self.assertIn("test jdk11 1/2: org.apache.cassandra.FooTest testB failure", text)
        self.assertIn("failed cells: 1\n  test jdk11 2/2", text)
        self.assertEqual(text, (out / "summary.txt").read_text())

    def test_a_cell_not_done_adds_no_results(self):
        out = self.tmp / "label"
        attempt = out / "cells/test_jdk11_python_3.8_no_amd64_1_1/attempt1/output"
        attempt.mkdir(parents=True)
        (attempt / "TEST-org.apache.cassandra.FooTest.xml").write_text(JUNIT)
        (out / "state.json").write_text(json.dumps({"cells": {
            "test_jdk11_python_3.8_no_amd64_1_1": {"name": "test jdk11 1/1", "status": "not run", "rc": None,
                                                   "attempt": 1, "kind": "test", "junit": 1}}}))
        text, result = summarise(out)
        self.assertEqual("FAILURE", result)
        self.assertIn("tests: passed 0 | failed 0 | skipped 0", text)

    def test_outcomes(self):
        out = self.tmp / "label"
        out.mkdir()
        cases = [
            ({"status": "done", "rc": 0, "kind": "jar"}, "SUCCESS"),
            ({"status": "done", "rc": 0, "kind": "test", "junit": 3}, "SUCCESS"),
            # a test step that left no test results
            ({"status": "done", "rc": 0, "kind": "test", "junit": 0}, "FAILURE"),
            ({"status": "lost", "rc": None, "kind": "test"}, "FAILURE"),
            ({"status": "done", "rc": 1, "kind": "build"}, "FAILURE"),
        ]
        for entry, expected in cases:
            with self.subTest(entry=entry):
                (out / "state.json").write_text(json.dumps({"cells": {"c": dict(entry, name="c")}}))
                self.assertEqual(expected, summarise(out)[1])


class TestExtraction(unittest.TestCase):

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.tmp)

    def extract(self, member, data=b""):
        buffer = io.BytesIO()
        with tarfile.open(fileobj=buffer, mode="w") as tar:
            tar.addfile(member, io.BytesIO(data) if member.isfile() else None)
        buffer.seek(0)
        destination = self.tmp / "out"
        destination.mkdir(exist_ok=True)
        with tarfile.open(fileobj=buffer) as tar:
            for m in tar:
                run_ci_local.extract_member(tar, m, destination)
        return destination

    def bad_members(self):
        outside = tarfile.TarInfo("../escape")
        absolute = tarfile.TarInfo("/tmp/absolute")
        link_out = tarfile.TarInfo("link")
        link_out.type, link_out.linkname = tarfile.SYMTYPE, "../../etc"
        absolute_link = tarfile.TarInfo("abs-link")
        absolute_link.type, absolute_link.linkname = tarfile.SYMTYPE, "/etc/passwd"
        device = tarfile.TarInfo("device")
        device.type = tarfile.CHRTYPE
        return [outside, absolute, link_out, absolute_link, device]

    def check(self):
        for member in self.bad_members():
            with self.subTest(member=member.name), self.assertRaises(tarfile.TarError):
                self.extract(member)
        good = tarfile.TarInfo("dir/file")
        good.size = 2
        self.assertEqual("ok", (self.extract(good, b"ok") / "dir/file").read_text())
        # a result holds no links, not even ones that stay inside
        for kind in (tarfile.SYMTYPE, tarfile.LNKTYPE):
            link = tarfile.TarInfo("dir/link")
            link.type, link.linkname = kind, "dir/file"
            with self.subTest(kind=kind), self.assertRaises(tarfile.TarError):
                self.extract(link)

    def test_without_the_data_filter(self):
        with patch.object(run_ci_local, "TAR_FILTER", {}):
            self.check()

    @unittest.skipUnless(hasattr(tarfile, "data_filter"), "this python has no tarfile.data_filter")
    def test_with_the_data_filter(self):
        self.check()


class StubRuntime:
    """
    A directory of stub docker and podman commands that log their arguments.  `ps -q` lists the ids in the file
    `ps`, `inspect <id>` prints the file `mounts.<id>`, or that the container is not running, and `run -d` prints a
    container id.
    """

    def __init__(self, directory, docker_version="Docker version 27.0.0, build stub", rootless="true"):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True)
        self.calls = self.directory / "calls"
        self.calls.write_text("")
        for name, version in (("docker", docker_version), ("podman", "podman version 5.4.2")):
            write_executable(self.directory / name, f"""#!/bin/bash
echo "{name} $*" >> "{self.calls}"
case "$1" in
  --version) echo "{version}" ;;
  info) case "$*" in *Rootless*) echo {rootless} ;; *CPUs*) echo 16 ;; *) echo "host: cpus: 16" ;; esac ;;
  image) exit 1 ;;
  ps) cat "{self.directory}/ps" 2>/dev/null ;;
  inspect)
    case "$*" in *State.Running*) echo false ;; *) cat "{self.directory}/mounts.${{@: -1}}" 2>/dev/null ;; esac ;;
  run) case " $* " in *" -d "*) echo "container-$RANDOM" ;; esac ;;
esac
exit 0
""")

    def env(self, **extra):
        return dict(os.environ, PATH=f"{self.directory}:{os.environ['PATH']}", **extra)


class TestJobScript(unittest.TestCase):
    """remote/job.sh and remote/podman/docker against stub container runtimes"""

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.tmp)
        self.home = self.tmp / "home"
        shutil.copytree(JOB_SH.parent, self.home / "bin/v")
        self.job = self.home / "bin/v/job.sh"

    def run_job(self, runtime, *args, **env):
        return subprocess.run([str(self.job), *args], env=runtime.env(**env), stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE, text=True, check=False)

    def test_runtime_detection(self):
        docker = StubRuntime(self.tmp / "docker-bin")
        self.assertEqual("docker", self.run_job(docker, "runtime").stdout.strip())
        podman = StubRuntime(self.tmp / "podman-bin", docker_version="podman version 5.4.2")
        self.assertEqual("podman rootless", self.run_job(podman, "runtime").stdout.strip())
        rootful = StubRuntime(self.tmp / "rootful-bin", docker_version="podman version 5.4.2", rootless="false")
        self.assertEqual("podman", self.run_job(rootful, "runtime").stdout.strip())

    def test_preflight_refuses_root(self):
        runtime = StubRuntime(self.tmp / "bin")
        self.assertEqual(0, self.run_job(runtime, "preflight").returncode)
        write_executable(runtime.directory / "id", '#!/bin/sh\n[ "$1" = "-u" ] && echo 0\n')
        result = self.run_job(runtime, "preflight")
        self.assertEqual(1, result.returncode)
        self.assertIn("runs as root", result.stdout)

    def test_capacity_and_slots(self):
        runtime = StubRuntime(self.tmp / "bin")
        self.run_job(runtime, "init", "4")
        self.assertEqual("4 4", self.run_job(runtime, "capacity").stdout.strip())
        # a host keeps its capacity: init sets one only where there is none
        self.run_job(runtime, "init", "6")
        self.assertEqual("4 4", self.run_job(runtime, "capacity").stdout.strip())
        self.run_job(runtime, "init", "")
        self.assertEqual("4 4", self.run_job(runtime, "capacity").stdout.strip())
        self.assertEqual(0, self.run_job(runtime, "set-capacity", "6").returncode)
        self.assertEqual("6 6", self.run_job(runtime, "capacity").stdout.strip())
        self.assertEqual(2, self.run_job(runtime, "set-capacity", "0").returncode)
        (self.home / "capacity").unlink()
        self.run_job(runtime, "init", "")
        self.assertEqual(os.cpu_count(), int((self.home / "capacity").read_text()))

    def test_names_are_single_path_components(self):
        runtime = StubRuntime(self.tmp / "bin")
        (self.home / "runs/keep/jobs").mkdir(parents=True)
        (self.home / "runs/keep/owner").write_text(OWNER + "\n")
        for label in ("", "..", ".", "a/b", "-x", ".hidden"):
            with self.subTest(label=label):
                result = self.run_job(runtime, "rmlabel", label, OWNER)
                self.assertEqual(2, result.returncode, result.stderr)
        self.assertTrue((self.home / "runs/keep").exists())
        # a label directory that leads out of runs/
        outside = self.tmp / "outside"
        (outside / "jobs").mkdir(parents=True)
        (outside / "owner").write_text(OWNER + "\n")
        (self.home / "runs/link").symlink_to(outside)
        self.assertEqual(79, self.run_job(runtime, "rmlabel", "link", OWNER).returncode)
        self.assertTrue(outside.exists())

    def test_owner(self):
        runtime = StubRuntime(self.tmp / "bin")
        self.run_job(runtime, "init", "2")
        self.assertEqual(0, self.run_job(runtime, "claim", "l", ONE, SESSION).returncode)
        self.assertEqual(0, self.run_job(runtime, "claim", "l", ONE, SESSION).returncode)
        self.assertEqual(76, self.run_job(runtime, "claim", "l", TWO, SESSION).returncode)
        (self.home / "runs/l/jobs/c.env").write_text("")
        self.assertEqual(76, self.run_job(runtime, "start", "l", "c", "1", TWO, "a1", SESSION).returncode)
        self.assertEqual(76, self.run_job(runtime, "rmlabel", "l", TWO).returncode)
        self.assertEqual(76, self.run_job(runtime, "kill", "l", TWO).returncode)
        self.assertTrue((self.home / "runs/l").exists())
        self.assertEqual(0, self.run_job(runtime, "rmlabel", "l", TWO, "force").returncode)
        self.assertFalse((self.home / "runs/l").exists())

    def start_cell(self, runtime, script, kind="build", units=2, tree=None, step="fake", test_regexp="", rc="0"):
        """Runs `job.sh start` of a cell whose .build/docker/<SCRIPT> is the given script, and waits for it"""
        if tree is None:
            tree = self.tmp / "tree"
            write_executable(tree / ".build/docker/fake.sh", script)
        run_dir = self.home / "runs/l"
        self.run_job(runtime, "init", "4")
        self.assertEqual(0, self.run_job(runtime, "claim", "l", OWNER, SESSION).returncode)
        cell = Cell(step, {'type': kind, 'splits': 1, 'timeout_hours': 1, 'script': 'fake.sh'}, units, '11', '3.8',
                    'no', test_regexp=test_regexp)
        with tarfile.open(run_dir / cell.base, "w") as tar:
            tar.add(str(tree), arcname=".")
        (run_dir / "jobs/c.env").write_text(cell.env(1, None))
        started = self.run_job(runtime, "start", "l", "c", str(units), OWNER, "a1", SESSION)
        self.assertEqual(0, started.returncode, started.stderr)
        for _ in range(200):
            status = self.run_job(runtime, "status", "l", "c", "a1").stdout.split()
            if status[0] == "done":
                break
            time.sleep(0.1)
        self.assertEqual(["done", rc], status)
        with tarfile.open(run_dir / "jobs/c.result.tar") as tar:
            return {m.name: tar.extractfile(m).read().decode(errors="replace") for m in tar.getmembers() if m.isfile()}

    def test_build_cell_on_docker(self):
        runtime = StubRuntime(self.tmp / "bin")
        result = self.start_cell(runtime, '#!/bin/sh\necho "args $* m2_dir=$m2_dir cpus=$docker_cpus jdk=$jdk"\n'
                                          'docker run --cpus=32 image\n')
        log = result["out/stage.log"]
        self.assertIn("container runtime docker", log)
        self.assertRegex(log, r"args 11 m2_dir=\S+/cassandra/build/m2 cpus=2 jdk=11")
        self.assertEqual("0\n", result["out/rc"])
        # no wrapper for docker
        self.assertIn("docker run --cpus=32 image", runtime.calls.read_text())
        self.assertFalse((self.home / "runs/l/jobs/c").exists())
        self.assertEqual("docker", (self.home / "runs/l/jobs/c.runtime").read_text().strip())

    def test_build_cell_on_rootless_podman(self):
        runtime = StubRuntime(self.tmp / "bin", docker_version="podman version 5.4.2")
        result = self.start_cell(runtime, '#!/bin/sh\ndocker run --cpus=32 -d image\n'
                                          'printf "FROM x\\nRUN adduser --uid 901743 u\\n" > Dockerfile\n'
                                          'docker build -t apache/x:1 -f Dockerfile --load .\n'
                                          'docker info | grep CPUs\n')
        self.assertIn("container runtime podman (rootless)", result["out/stage.log"])
        self.assertIn(" CPUs: 16", result["out/stage.log"])
        calls = runtime.calls.read_text()
        self.assertIn("podman run --userns=keep-id --passwd=false --cpus=2 -d image", calls)
        build = [line for line in calls.splitlines() if line.startswith("podman build")]
        self.assertEqual(1, len(build))
        self.assertNotIn("--load", build[0])
        self.assertNotIn("Dockerfile .", build[0], "a rootless build uses a copy with the uid remapped")
        # the detached container the wrapper recorded is stopped when the cell ends
        container = (self.home / "runs/l/jobs/c.containers").read_text().strip()
        self.assertRegex(container, r"^container-\d+$")
        self.assertIn(f"podman stop -t 5 {container}", calls)

    def test_cell_stops_only_its_own_containers(self):
        runtime = StubRuntime(self.tmp / "bin")
        jobs = self.home / "runs/l/jobs"
        jobs.mkdir(parents=True)
        (self.home / "runs/l/owner").write_text(OWNER + "\n")
        (jobs / "c.runtime").write_text("docker\n")
        (jobs / "c.attempt").write_text("a1\n")
        (jobs / "c.containers").write_text("recorded\n")
        (runtime.directory / "ps").write_text("mine\nsibling\nother\n")
        (runtime.directory / "mounts.mine").write_text(f"/elsewhere\n{jobs}/c/cassandra/build dir\n")
        (runtime.directory / "mounts.sibling").write_text(f"{jobs}/c2/cassandra\n")
        (runtime.directory / "mounts.other").write_text("/home/other\n")
        # a stop of an earlier attempt does nothing
        self.assertEqual(0, self.run_job(runtime, "stop", "l", "c", "a0").returncode)
        self.assertNotIn("docker stop", runtime.calls.read_text())
        self.assertEqual(0, self.run_job(runtime, "stop", "l", "c", "a1").returncode)
        stopped = [line.split()[-1] for line in runtime.calls.read_text().splitlines()
                   if line.startswith("docker stop")]
        self.assertEqual(["recorded", "mine"], stopped)

    def test_slot_locks_end_with_the_cell(self):
        # a process the cell leaves behind, as podman leaves its helpers, must not keep the cell's slots
        runtime = StubRuntime(self.tmp / "bin")
        pid_file = self.tmp / "leftover.pid"
        self.start_cell(runtime, '#!/bin/bash\nsetsid sleep 60 > /dev/null 2>&1 < /dev/null &\n'
                                 f'echo $! > "{pid_file}"\n', units=4)
        leftover = int(pid_file.read_text())
        self.addCleanup(lambda: subprocess.run(["kill", str(leftover)], check=False))
        self.wait_for_capacity(runtime, "4 4")
        self.assertTrue(Path(f"/proc/{leftover}").exists())

    def prepare_cell(self, runtime, script, label="l", owner=OWNER):
        tree = self.tmp / "tree"
        write_executable(tree / ".build/docker/fake.sh", script)
        run_dir = self.home / "runs" / label
        self.run_job(runtime, "init", "4")
        self.assertEqual(0, self.run_job(runtime, "claim", label, owner, SESSION).returncode)
        cell = Cell("fake", {'type': 'build', 'splits': 1, 'timeout_hours': 1, 'script': 'fake.sh'}, 1, '11', '3.8',
                    'no')
        with tarfile.open(run_dir / cell.base, "w") as tar:
            tar.add(str(tree), arcname=".")
        (run_dir / "jobs/c.env").write_text(cell.env(1, None))
        return run_dir

    def wait_for(self, condition):
        for _ in range(100):
            if condition():
                return
            time.sleep(0.1)
        self.fail("timed out")

    def test_one_runner_per_cell(self):
        runtime = StubRuntime(self.tmp / "bin")
        run_dir = self.prepare_cell(runtime, "#!/bin/sh\nsleep 2\n")
        self.assertEqual(0, self.run_job(runtime, "start", "l", "c", "1", OWNER, "a1", SESSION).returncode)
        self.wait_for(lambda: self.run_job(runtime, "status", "l", "c", "a1").stdout.strip() == "running"
                      and (run_dir / "jobs/c.pid").exists())
        # another attempt may not start while this one runs
        self.assertEqual(77, self.run_job(runtime, "start", "l", "c", "1", OWNER, "a2", SESSION).returncode)
        self.assertEqual("unknown", self.run_job(runtime, "status", "l", "c", "a2").stdout.strip())
        # an attempt that is no longer the latest writes no result
        (run_dir / "jobs/c.attempt").write_text("a3\n")
        self.wait_for(lambda: self.run_job(runtime, "status", "l", "c", "a3").stdout.strip() == "lost")
        self.assertFalse((run_dir / "jobs/c.rc").exists())
        self.assertFalse((run_dir / "jobs/c.result.tar").exists())
        self.assertEqual("unknown", self.run_job(runtime, "status", "l", "c", "a1").stdout.strip())

    def test_no_label_is_taken_over(self):
        runtime = StubRuntime(self.tmp / "bin")
        run_dir = self.prepare_cell(runtime, "#!/bin/sh\nsleep 60\n", owner=ONE)
        self.assertEqual(0, self.run_job(runtime, "start", "l", "c", "1", ONE, "a1", SESSION).returncode)
        self.wait_for(lambda: self.run_job(runtime, "status", "l", "c", "a1").stdout.strip() == "running"
                      and (run_dir / "jobs/c.pid").exists())
        self.assertEqual(76, self.run_job(runtime, "claim", "l", TWO, SESSION).returncode)
        self.assertEqual(76, self.run_job(runtime, "claim", "l", TWO, SESSION, "force").returncode)
        self.assertEqual(76, self.run_job(runtime, "kill", "l", TWO).returncode)
        # nor removed while one of its cells runs
        self.assertEqual(1, self.run_job(runtime, "rmlabel", "l", TWO, "force").returncode)
        self.assertEqual("running", self.run_job(runtime, "status", "l", "c", "a1").stdout.strip())
        self.assertEqual(ONE, (run_dir / "owner").read_text().strip())
        self.assertEqual(0, self.run_job(runtime, "kill", "l", ONE).returncode)

    def test_claim_makes_a_new_directory_only(self):
        runtime = StubRuntime(self.tmp / "bin")
        foreign = self.home / "runs/l"
        (foreign / "data").mkdir(parents=True)
        self.assertEqual(76, self.run_job(runtime, "claim", "l", OWNER, SESSION).returncode)
        self.assertFalse((foreign / "owner").exists())
        (foreign / "owner").write_text("not-a-marker\n")
        self.assertEqual(76, self.run_job(runtime, "claim", "l", OWNER, SESSION).returncode)
        self.assertEqual(76, self.run_job(runtime, "rmlabel", "l", OWNER, "force").returncode)
        self.assertTrue((foreign / "data").exists())
        self.assertEqual("", self.run_job(runtime, "labels").stdout)

    def test_an_interrupted_claim_leaves_no_label(self):
        runtime = StubRuntime(self.tmp / "bin")
        self.run_job(runtime, "init", "2")
        write_executable(runtime.directory / "mv", "#!/bin/sh\nexit 1\n")
        self.assertEqual(1, self.run_job(runtime, "claim", "l", OWNER, SESSION).returncode)
        self.assertEqual([], os.listdir(self.home / "runs"))
        # what a killed claim left aside is neither a label nor in the way of the next claim
        (self.home / "runs/.claim-l/jobs").mkdir(parents=True)
        (runtime.directory / "mv").unlink()
        self.assertEqual("", self.run_job(runtime, "labels").stdout)
        self.assertEqual(0, self.run_job(runtime, "claim", "l", OWNER, SESSION).returncode)
        self.assertEqual(["l"], os.listdir(self.home / "runs"))
        self.assertEqual(OWNER, (self.home / "runs/l/owner").read_text().strip())

    def test_a_stopped_attempt_never_starts(self):
        runtime = StubRuntime(self.tmp / "bin")
        self.prepare_cell(runtime, "#!/bin/sh\nsleep 30\n")
        # a start that arrives after the driver stopped its attempt
        self.assertEqual("not running", self.run_job(runtime, "stop", "l", "c", "a1").stdout.strip())
        self.assertEqual(78, self.run_job(runtime, "start", "l", "c", "1", OWNER, "a1", SESSION).returncode)
        self.assertEqual(0, self.run_job(runtime, "start", "l", "c", "1", OWNER, "a2", SESSION).returncode)
        self.wait_for(lambda: self.run_job(runtime, "status", "l", "c", "a2").stdout.strip() == "running")
        self.assertEqual("stopped", self.run_job(runtime, "stop", "l", "c", "a2").stdout.strip())
        self.assertEqual("lost", self.run_job(runtime, "status", "l", "c", "a2").stdout.strip())
        self.assertEqual(78, self.run_job(runtime, "start", "l", "c", "1", OWNER, "a2", SESSION).returncode)

    def test_a_replaced_attempt_never_starts(self):
        runtime = StubRuntime(self.tmp / "bin")
        self.prepare_cell(runtime, "#!/bin/sh\nexit 0\n")
        for attempt in ("b1", "b2"):
            self.assertEqual(0, self.run_job(runtime, "start", "l", "c", "1", OWNER, attempt, SESSION).returncode)
            self.wait_for(lambda: self.run_job(runtime, "status", "l", "c", attempt).stdout.strip() == "done 0")
        self.assertEqual(78, self.run_job(runtime, "start", "l", "c", "1", OWNER, "b1", SESSION).returncode)
        self.assertEqual("finished", self.run_job(runtime, "stop", "l", "c", "b2").stdout.strip())

    def test_only_the_labels_session_starts_cells(self):
        runtime = StubRuntime(self.tmp / "bin")
        run_dir = self.prepare_cell(runtime, "#!/bin/sh\nexit 0\n")
        env = (run_dir / "jobs/c.env").read_text()
        other = "5e55105e55105e56"
        self.assertEqual(78, self.run_job(runtime, "start", "l", "c", "1", OWNER, "a1", other).returncode)
        # a driver that takes over the label
        self.assertEqual(0, self.run_job(runtime, "kill", "l", OWNER, other).returncode)
        (run_dir / "jobs/c.env").write_text(env)
        self.assertEqual(78, self.run_job(runtime, "start", "l", "c", "1", OWNER, "a1", SESSION).returncode)
        self.assertEqual(0, self.run_job(runtime, "start", "l", "c", "1", OWNER, "a1", other).returncode)

    def test_kill_waits_for_the_cells_and_discards_what_they_left(self):
        runtime = StubRuntime(self.tmp / "bin")
        run_dir = self.prepare_cell(runtime, "#!/bin/sh\nsleep 30\n")
        self.assertEqual(0, self.run_job(runtime, "start", "l", "c", "1", OWNER, "a1", SESSION).returncode)
        self.wait_for(lambda: (run_dir / "jobs/c.pid").exists())
        pid = int((run_dir / "jobs/c.pid").read_text())
        killed = self.run_job(runtime, "kill", "l", OWNER, "5e55105e55105e56")
        self.assertEqual((0, "1"), (killed.returncode, killed.stdout.strip()))
        self.assertFalse(Path(f"/proc/{pid}").exists() and "_run" in Path(f"/proc/{pid}/cmdline").read_text())
        self.assertEqual(["c.attempt"], os.listdir(run_dir / "jobs"))
        self.assertEqual("5e55105e55105e56", (run_dir / "session").read_text().strip())

    def test_a_starting_cell_counts_as_running(self):
        runtime = StubRuntime(self.tmp / "bin")
        run_dir = self.prepare_cell(runtime, "#!/bin/sh\nsleep 30\n")
        self.assertEqual(0, self.run_job(runtime, "start", "l", "c", "1", OWNER, "a1", SESSION).returncode)
        self.wait_for(lambda: (run_dir / "jobs/c.pid").exists())
        pid = int((run_dir / "jobs/c.pid").read_text())
        self.addCleanup(lambda: self.run_job(runtime, "kill", "l", OWNER))
        lock_file = self.home / "locks/label-l.lock"
        # the runner does not keep the label lock that start held
        open_files = []
        for fd in os.listdir(f"/proc/{pid}/fd"):
            with contextlib.suppress(FileNotFoundError):
                open_files.append(os.readlink(f"/proc/{pid}/fd/{fd}"))
        self.assertNotIn(str(lock_file), open_files)
        with open(lock_file, "w") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            with self.assertRaises(subprocess.TimeoutExpired):
                subprocess.run([str(self.job), "labels"], env=runtime.env(), stdout=subprocess.PIPE, timeout=2)
        self.assertRegex(self.run_job(runtime, "labels").stdout, r"^l 1 ")

    def test_clean_says_when_it_cannot_remove(self):
        runtime = StubRuntime(self.tmp / "bin")
        run_dir = self.prepare_cell(runtime, "#!/bin/sh\nexit 0\n")
        self.assertEqual(0, self.run_job(runtime, "start", "l", "c", "1", OWNER, "a1", SESSION).returncode)
        self.wait_for(lambda: self.run_job(runtime, "status", "l", "c", "a1").stdout.strip() == "done 0")
        (run_dir / "jobs").chmod(0o555)
        self.addCleanup((run_dir / "jobs").chmod, 0o755)
        self.assertEqual(1, self.run_job(runtime, "clean", "l", "c", "a1", SESSION).returncode)

    def test_rmlabel_removes_what_an_interrupted_claim_left(self):
        runtime = StubRuntime(self.tmp / "bin")
        (self.home / "runs/.claim-l/jobs").mkdir(parents=True)
        self.assertEqual(0, self.run_job(runtime, "rmlabel", "l", OWNER).returncode)
        self.assertFalse((self.home / "runs/.claim-l").exists())

    def put(self, runtime, data, *args):
        return subprocess.run([str(self.job), "put", *args], env=runtime.env(), input=data, stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE, check=False)

    def test_writes_into_a_label_come_from_its_session_only(self):
        runtime = StubRuntime(self.tmp / "bin")
        run_dir = self.prepare_cell(runtime, "#!/bin/sh\nexit 0\n")
        other = "5e55105e55105e56"
        self.assertEqual(0, self.put(runtime, b"abc", "l", SESSION, "src.tar", "3", "v1").returncode)
        stash, version = run_dir / "src.tar", run_dir / "src.tar.version"
        self.assertEqual("abc v1\n", stash.read_text() + " " + version.read_text())
        # a write of an earlier session, and one cut short, change nothing
        self.assertEqual(78, self.put(runtime, b"xyz", "l", other, "src.tar", "3", "v2").returncode)
        self.assertEqual(1, self.put(runtime, b"xy", "l", SESSION, "src.tar", "3", "v2").returncode)
        self.assertEqual(78, self.run_job(runtime, "mark", "l", other, "src.tar", "v2").returncode)
        self.assertEqual(78, self.put(runtime, b"E=1\n", "l", other, "jobs/c.env", "4").returncode)
        self.assertEqual("abc v1\n", stash.read_text() + " " + version.read_text())
        self.assertEqual(0, self.run_job(runtime, "mark", "l", SESSION, "src.tar", "v3").returncode)
        self.assertEqual("v3", (run_dir / "src.tar.version").read_text().strip())
        self.assertEqual([], list(run_dir.glob(".put-*")))

    def test_clean_is_of_the_labels_session(self):
        runtime = StubRuntime(self.tmp / "bin")
        run_dir = self.prepare_cell(runtime, "#!/bin/sh\nexit 0\n")
        self.assertEqual(0, self.run_job(runtime, "start", "l", "c", "1", OWNER, "a1", SESSION).returncode)
        self.wait_for(lambda: self.run_job(runtime, "status", "l", "c", "a1").stdout.strip() == "done 0")
        self.assertEqual(78, self.run_job(runtime, "clean", "l", "c", "a1", "5e55105e55105e56").returncode)
        self.assertTrue((run_dir / "jobs/c.result.tar").exists())
        self.assertEqual(0, self.run_job(runtime, "clean", "l", "c", "a1", SESSION).returncode)
        self.assertFalse((run_dir / "jobs/c.result.tar").exists())

    def test_kill_leaves_a_directory_it_did_not_make_alone(self):
        runtime = StubRuntime(self.tmp / "bin")
        jobs = self.home / "runs/x/jobs"
        jobs.mkdir(parents=True)
        runner = fake_runner("x", "c")
        self.addCleanup(runner.kill)
        (jobs / "c.pid").write_text(f"{runner.pid}\n")
        self.assertEqual(76, self.run_job(runtime, "kill", "x", OWNER, SESSION).returncode)
        self.assertIsNone(runner.poll())
        self.assertEqual(["c.pid"], os.listdir(jobs))
        self.assertFalse((self.home / "runs/x/session").exists())

    def test_a_stop_the_runtime_cannot_confirm_fails(self):
        runtime = StubRuntime(self.tmp / "bin")
        run_dir = self.home / "runs/l"
        (run_dir / "jobs").mkdir(parents=True)
        (run_dir / "owner").write_text(OWNER + "\n")
        (run_dir / "jobs/c.attempt").write_text("a1\n")
        (run_dir / "jobs/c.runtime").write_text("docker\n")
        (run_dir / "jobs/c.containers").write_text("stuck\n")
        for name, script in (("ps", '[ "$1" = ps ] && exit 1\n'),
                             ("stop", '[ "$1" = stop ] && exit 1\n[ "$1" = inspect ] && echo true\n'),
                             # a container of the cell, or not, whose state the runtime does not give
                             ("inspect", '[ "$1" = ps ] && echo other\n[ "$1" = inspect ] && exit 1\n')):
            with self.subTest(failing=name):
                write_executable(runtime.directory / "docker", f"#!/bin/sh\n{script}exit 0\n")
                self.assertEqual(1, self.run_job(runtime, "stop", "l", "c", "a1").returncode)
                self.assertEqual(1, self.run_job(runtime, "kill", "l", OWNER, SESSION).returncode)
                self.assertEqual({"c.attempt", "c.retired", "c.runtime", "c.containers"},
                                 set(os.listdir(run_dir / "jobs")))
        write_executable(runtime.directory / "docker", '#!/bin/sh\n[ "$1" = inspect ] && echo false\nexit 0\n')
        self.assertEqual("not running", self.run_job(runtime, "stop", "l", "c", "a1").stdout.strip())
        self.assertEqual(0, self.run_job(runtime, "kill", "l", OWNER, SESSION).returncode)
        self.assertEqual(["c.attempt", "c.retired"], sorted(os.listdir(run_dir / "jobs")))

    def test_a_stop_as_the_runner_ends_is_not_an_interruption(self):
        runtime = StubRuntime(self.tmp / "bin")
        jobs = self.home / "runs/l/jobs"
        jobs.mkdir(parents=True)
        (self.home / "runs/l/owner").write_text(OWNER + "\n")
        (jobs / "c.attempt").write_text("a1\n")
        # a runner that writes its result when told to stop
        runner = subprocess.Popen(["setsid", "bash", "-c", f'exec -a "job.sh _run l c x" bash -c '
                                   f'\'trap "echo 0 > {jobs}/c.rc; exit 0" TERM; while :; do sleep 0.1; done\''])
        self.addCleanup(runner.kill)
        (jobs / "c.pid").write_text(f"{runner.pid}\n")
        self.wait_for(lambda: "job.sh _run l c" in Path(f"/proc/{runner.pid}/cmdline").read_text().replace("\0", " "))
        self.assertEqual("finished", self.run_job(runtime, "stop", "l", "c", "a1").stdout.strip())

    def test_a_replaced_stash_never_has_the_version_of_another(self):
        runtime = StubRuntime(self.tmp / "bin")
        run_dir = self.prepare_cell(runtime, "#!/bin/sh\nexit 0\n")
        self.assertEqual(0, self.put(runtime, b"aaa", "l", SESSION, "src.tar", "3", "va").returncode)
        # an upload that dies once its file is in place, before its version is
        write_executable(runtime.directory / "mv", '#!/bin/bash\n/bin/mv "$@"\n'
                                                   'case "$2" in *.put-*) kill -9 $PPID ;; esac\n')
        self.assertNotEqual(0, self.put(runtime, b"bbb", "l", SESSION, "src.tar", "3", "vb").returncode)
        (runtime.directory / "mv").unlink()
        self.assertEqual("bbb", (run_dir / "src.tar").read_text())
        self.assertFalse((run_dir / "src.tar.version").exists())

    def test_a_finished_attempt_stops_only_with_its_containers(self):
        runtime = StubRuntime(self.tmp / "bin")
        jobs = self.home / "runs/l/jobs"
        jobs.mkdir(parents=True)
        (self.home / "runs/l/owner").write_text(OWNER + "\n")
        for name, text in (("attempt", "a1"), ("rc", "0"), ("runtime", "docker"), ("containers", "stuck")):
            (jobs / f"c.{name}").write_text(text + "\n")
        write_executable(runtime.directory / "docker", '#!/bin/sh\n[ "$1" = stop ] && exit 1\n'
                                                       '[ "$1" = inspect ] && echo true\nexit 0\n')
        self.assertEqual(1, self.run_job(runtime, "stop", "l", "c", "a1").returncode)
        self.assertTrue((jobs / "c.containers").exists())

    def test_a_start_waits_for_the_containers_of_the_attempt_before(self):
        runtime = StubRuntime(self.tmp / "bin")
        run_dir = self.prepare_cell(runtime, "#!/bin/sh\nexit 0\n")
        jobs = run_dir / "jobs"
        for name, text in (("attempt", "a1"), ("rc", "1"), ("result.tar", "x"), ("runtime", "docker"),
                           ("containers", "stuck")):
            (jobs / f"c.{name}").write_text(text + "\n")
        write_executable(runtime.directory / "docker", '#!/bin/sh\n[ "$1" = stop ] && exit 1\n'
                                                       '[ "$1" = inspect ] && echo true\nexit 0\n')
        self.assertEqual(77, self.run_job(runtime, "start", "l", "c", "1", OWNER, "a2", SESSION).returncode)
        self.assertEqual(("a1\n", "x\n"), ((jobs / "c.attempt").read_text(), (jobs / "c.result.tar").read_text()))

    def test_claim_never_changes_a_labels_session(self):
        runtime = StubRuntime(self.tmp / "bin")
        run_dir = self.prepare_cell(runtime, "#!/bin/sh\nexit 0\n")
        other = "5e55105e55105e56"
        self.assertEqual(0, self.run_job(runtime, "claim", "l", OWNER, SESSION).returncode)
        self.assertEqual(78, self.run_job(runtime, "claim", "l", OWNER, other).returncode)
        self.assertEqual(SESSION, (run_dir / "session").read_text().strip())
        # taken over by a resume, then a claim of the earlier run that arrives late
        self.assertEqual(0, self.run_job(runtime, "kill", "l", OWNER, other).returncode)
        self.assertEqual(0, self.run_job(runtime, "claim", "l", OWNER, other).returncode)
        self.assertEqual(78, self.run_job(runtime, "claim", "l", OWNER, SESSION).returncode)
        self.assertEqual(other, (run_dir / "session").read_text().strip())

    def test_usage_names_every_command(self):
        usage = self.run_job(StubRuntime(self.tmp / "bin")).stderr
        for command in ("owner", "claim", "put", "mark", "start", "stop", "clean", "kill", "rmlabel"):
            self.assertIn(command, usage)

    def test_an_attempt_starts_once(self):
        runtime = StubRuntime(self.tmp / "bin")
        run_dir = self.prepare_cell(runtime, "#!/bin/sh\nsleep 1\n")
        self.assertEqual(0, self.run_job(runtime, "start", "l", "c", "1", OWNER, "a1", SESSION).returncode)
        self.wait_for(lambda: (run_dir / "jobs/c.pid").exists())
        pid = (run_dir / "jobs/c.pid").read_text()
        again = self.run_job(runtime, "start", "l", "c", "1", OWNER, "a1", SESSION)
        self.assertEqual((0, "attempt a1 of c has started already\n"), (again.returncode, again.stdout))
        self.assertEqual(pid, (run_dir / "jobs/c.pid").read_text())
        self.wait_for(lambda: self.run_job(runtime, "status", "l", "c", "a1").stdout.strip() == "done 0")
        self.assertEqual(0, self.run_job(runtime, "start", "l", "c", "1", OWNER, "a1", SESSION).returncode)
        self.assertEqual("done 0", self.run_job(runtime, "status", "l", "c", "a1").stdout.strip())
        self.assertEqual(pid, (run_dir / "jobs/c.pid").read_text())

    def test_each_preflight_probes_its_own_file(self):
        runtime = StubRuntime(self.tmp / "bin")
        for _ in range(2):
            self.assertEqual(0, self.run_job(runtime, "preflight").returncode)
        probes = re.findall(r"touch /probe/(\S+) ", runtime.calls.read_text())
        self.assertEqual(2, len(set(probes)), probes)
        for probe in probes:
            self.assertRegex(probe, r"^\.preflight-\d+-\d+$")

    def test_a_failed_image_pull_may_pass(self):
        runtime = StubRuntime(self.tmp / "bin")
        write_executable(runtime.directory / "docker", '#!/bin/sh\n[ "$1" = image ] || [ "$1" = pull ] && exit 1\n'
                                                       'echo "Docker version 27.0.0, build stub"\n')
        result = self.run_job(runtime, "preflight")
        self.assertEqual(3, result.returncode)
        self.assertIn("cannot pull alpine:3.19.1", result.stdout)

    def test_rmlabel_needs_the_owner_marker(self):
        runtime = StubRuntime(self.tmp / "bin")
        (self.home / "runs/x/jobs").mkdir(parents=True)
        self.assertEqual(76, self.run_job(runtime, "rmlabel", "x", OWNER).returncode)
        self.assertTrue((self.home / "runs/x").exists())

    def test_preflight_needs_a_working_runtime(self):
        runtime = StubRuntime(self.tmp / "bin")
        write_executable(runtime.directory / "docker", '#!/bin/sh\n[ "$1" = info ] && exit 1\n'
                                                       'echo "Docker version 27.0.0, build stub"\n')
        result = self.run_job(runtime, "preflight")
        # a runtime that does not answer may answer later
        self.assertEqual(3, result.returncode)
        self.assertIn("`docker info` fails", result.stdout)

    def test_a_lowered_capacity_counts_every_held_slot(self):
        runtime = StubRuntime(self.tmp / "bin")
        self.run_job(runtime, "init", "8")
        self.run_job(runtime, "capacity")
        holder = subprocess.Popen(["flock", str(self.home / "slots/slot.6"), "sleep", "30"])
        self.addCleanup(holder.wait)
        self.addCleanup(holder.kill)
        self.wait_for(lambda: self.run_job(runtime, "capacity").stdout.strip() == "7 8")
        self.run_job(runtime, "set-capacity", "4")
        # slot 6 is above the new capacity, and still in use
        self.assertEqual("3 4", self.run_job(runtime, "capacity").stdout.strip())
        (self.home / "runs/l/jobs").mkdir(parents=True)
        (self.home / "runs/l/session").write_text(SESSION + "\n")
        (self.home / "runs/l/jobs/c.env").write_text("")
        self.assertEqual(75, self.run_job(runtime, "start", "l", "c", "4", OWNER, "a1", SESSION).returncode)

    def test_preflight_needs_a_writable_bind_mount(self):
        runtime = StubRuntime(self.tmp / "bin")
        write_executable(runtime.directory / "docker", '#!/bin/sh\n'
                         'case "$1" in --version) echo "Docker version 27.0.0, build stub" ;; '
                         'run) echo "touch: /probe/.preflight: Permission denied" >&2; exit 1 ;; esac\n')
        result = self.run_job(runtime, "preflight")
        self.assertEqual(1, result.returncode)
        self.assertIn("cannot write to a bind mount", result.stdout)
        self.assertIn("Permission denied", result.stdout)

    def test_status_is_not_fooled_by_a_reused_pid(self):
        runtime = StubRuntime(self.tmp / "bin")
        jobs = self.home / "runs/l/jobs"
        jobs.mkdir(parents=True)
        (jobs / "c.pid").write_text(f"{os.getpid()}\n")
        (jobs / "c.started").touch()
        (jobs / "c.attempt").write_text("a1\n")
        self.assertEqual("lost", self.run_job(runtime, "status", "l", "c", "a1").stdout.strip())

    def test_arch(self):
        runtime = StubRuntime(self.tmp / "bin")
        self.assertIn(self.run_job(runtime, "arch").stdout.strip(), ("amd64", "arm64"))

    def test_test_regexp_reaches_run_tests_through_the_container_shell(self):
        self.check_test_regexp(older_run_tests=False)

    def test_test_regexp_with_a_run_tests_that_does_not_quote_it(self):
        self.check_test_regexp(older_run_tests=True)

    def check_test_regexp(self, older_run_tests):
        # the real .build/docker/run-tests.sh, with a stub docker whose exec runs the command in a shell, as a
        # container does
        tree = self.tmp / "tree"
        for path in ("build.xml", ".build/run-tests.sh", ".build/run-python-dtests.sh", ".build/docker/run-tests.sh",
                     ".build/docker/ubuntu-test.docker"):
            (tree / path).parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(CASSANDRA_DIR / path, tree / path)
        if older_run_tests:
            run_tests = tree / ".build/docker/run-tests.sh"
            quoted = '''test_name_regexp_arg="-t $(printf '%q' "${test_name_regexp}")"'''
            self.assertIn(quoted, run_tests.read_text())
            run_tests.write_text(run_tests.read_text().replace(quoted, 'test_name_regexp_arg="-t ${test_name_regexp}"'))
        write_executable(tree / ".build/docker/_set_java.sh", "true\n")
        write_executable(tree / ".build/docker/_docker_init_tests.sh",
                         '#!/bin/bash\nfor a in "$@"; do echo "init-arg: $a"; done\n')
        runtime = StubRuntime(self.tmp / "bin")
        write_executable(runtime.directory / "docker", """#!/bin/bash
case "$1" in
  --version) echo "Docker version 27.0.0, build stub" ;;
  info) echo " CPUs: 8" ;;
  images) echo "stub-image-id" ;;
  run) case "$*" in *nproc*) echo 8 ;; *free*) echo "Mem: 34359738368 0 0" ;; *) echo stub-container ;; esac ;;
  exec) case "${@: -1}" in *_docker_init_tests.sh*) CASSANDRA_DIR="$PWD" bash -c "${@: -1}" ;; esac ;;
esac
exit 0
""")
        regexp = "FooTest|Bar.*Test$"
        result = self.start_cell(runtime, None, kind="test", tree=tree, step="test", test_regexp=regexp)
        self.assertIn(f"init-arg: {regexp}\n", result["out/stage.log"], result["out/stage.log"])

    def wait_for_capacity(self, runtime, expected):
        # the slot holder notices the runner's exit within a second
        for _ in range(50):
            if self.run_job(runtime, "capacity").stdout.strip() == expected:
                break
            time.sleep(0.1)
        self.assertEqual(expected, self.run_job(runtime, "capacity").stdout.strip())

    def test_failing_cell_and_cleanup(self):
        runtime = StubRuntime(self.tmp / "bin")
        self.start_cell(runtime, "#!/bin/sh\nexit 3\n", units=1, rc="3")
        self.wait_for_capacity(runtime, "4 4")
        label, live, _, size, owner = self.run_job(runtime, "labels").stdout.split()
        self.assertEqual(("l", "0", OWNER), (label, live, owner))
        self.assertGreater(int(size), 0)
        self.assertEqual(0, self.run_job(runtime, "rmlabel", "l", OWNER).returncode)
        self.assertFalse((self.home / "runs/l").exists())

    def test_full_host_and_reservations(self):
        runtime = StubRuntime(self.tmp / "bin")
        run_dir = self.home / "runs/l"
        (run_dir / "jobs").mkdir(parents=True)
        (run_dir / "session").write_text(SESSION + "\n")
        (run_dir / "jobs/big.env").write_text("")
        self.run_job(runtime, "init", "2")
        # a held slot leaves one unit
        holder = subprocess.Popen(["flock", str(self.home / "slots/slot.0"), "sleep", "30"])
        self.addCleanup(holder.wait)
        self.addCleanup(holder.kill)
        for _ in range(50):
            if self.run_job(runtime, "capacity").stdout.split()[0] == "1":
                break
            time.sleep(0.1)
        self.assertEqual(75, self.run_job(runtime, "start", "l", "big", "2", OWNER, "a1", SESSION).returncode)
        self.assertFalse((self.home / "reservations/l.big").exists())
        reserved = self.run_job(runtime, "start", "l", "big", "2", OWNER, "a1", SESSION, "reserve")
        self.assertEqual(75, reserved.returncode)
        self.assertEqual("2", (self.home / "reservations/l.big").read_text().strip())
        # another cell may not take the last unit while the reservation stands
        (run_dir / "jobs/small.env").write_text("")
        self.assertEqual(75, self.run_job(runtime, "start", "l", "small", "1", OWNER, "a1", SESSION).returncode)
        self.assertEqual(0, self.run_job(runtime, "unreserve", "l", "big").returncode)
        self.assertFalse((self.home / "reservations/l.big").exists())


class FakeHost:
    """A host whose run() answers from a function of the command"""

    def __init__(self, answer, name="fake"):
        self.name, self.answer, self.commands = name, answer, []
        self.remote = run_ci_local.Remote(".run-ci-local")

    def run(self, command, check=True, **_):
        self.commands.append(command)
        return self.answer(command)

    def __repr__(self):
        return self.name


class TestLocalRun(unittest.TestCase):
    """The driver end to end, on localhost, with a fake checkout whose .build/docker scripts write JUnit files"""

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.tmp)
        self.runtime = StubRuntime(self.tmp / "stub-bin")
        self.config = self.tmp / "run-ci-local.conf"
        self.config.write_text("")
        environment = patch.dict(os.environ, {"PATH": self.runtime.env()["PATH"],
                                              "RUN_CI_LOCAL_CONFIG": str(self.config)})
        environment.start()
        self.addCleanup(environment.stop)
        self.src = self.tmp / "src"
        (self.src / ".jenkins").mkdir(parents=True)
        shutil.copy(JENKINSFILE, self.src / ".jenkins/Jenkinsfile")
        (self.src / "build.xml").write_text('<project>\n  <property name="java.default" value="11" />\n'
                                            '  <property name="java.supported" value="11,17" />\n</project>\n')
        write_executable(self.src / ".build/docker/build-jars.sh",
                         '#!/bin/sh\nmkdir -p build && echo jar > build/jar.txt\n')
        write_executable(self.src / ".build/docker/check-code.sh", '#!/bin/sh\ntest -f build/jar.txt\n')
        flaky = self.tmp / "flaky"
        write_executable(self.src / ".build/docker/run-tests.sh", f"""#!/bin/bash
[ -f build/jar.txt ] || exit 5
# the first attempt fails
if [ ! -f "{flaky}" ]; then touch "{flaky}"; exit 4; fi
mkdir -p build/test/output/$2
cat > build/test/output/$2/TEST-org.apache.cassandra.StressTest.xml <<EOF
{JUNIT}
EOF
echo "[{{\\"benchmark\\": \\"$2\\"}}]" > build/test/output/$2/jmh-result.json
""")
        write_executable(self.src / ".build/docker/_docker_run.sh", """#!/bin/bash
if [ "$2" == "ci/generate-ci-summary.sh" ] ; then
  echo "<html>tag=$BUILD_TAG repository=$repository branch=$branch profile=$profile regexp=$profile_custom_regexp" \
       "architecture=$architecture jdk=$jdk dtest=$dtest_repository $dtest_branch" \
       "$(find build/test/output -name '*.xml' | sort)</html>" > build/ci_summary.html
fi
""")
        self.commit("src", init=True)
        self.hosts = self.tmp / "hosts"
        self.hosts.write_text("localhost 8\n")
        self.results = self.tmp / "results"
        self.remote_home = str(self.tmp / "remote")

    def commit(self, message, init=False):
        steps = [["init", "-q", "-b", "trunk"]] if init else []
        steps += [["add", "."], ["-c", "user.name=t", "-c", "user.email=t@example.com", "commit", "-q", "-m", message]]
        for args in steps:
            subprocess.run(["git", "-C", str(self.src), *args], check=True)

    def run_ci_local(self, *args):
        """(stdout, exit code)"""
        real_sleep = time.sleep
        with patch("run_ci_local.time.sleep", lambda s: real_sleep(min(s, 0.2))), \
                contextlib.redirect_stdout(io.StringIO()) as out:
            code = run_ci_local.main(["--hosts", str(self.hosts), "--remote-home", self.remote_home,
                                      "--results-dir", str(self.results), *args])
        return out.getvalue(), code

    def run_stress(self, label, *args):
        return self.run_ci_local("run", "-l", label, "--src", str(self.src), "-p", "custom", "-e", "stress-test",
                                 "-j", "11", *args)

    def cell_state(self, label, cell_id):
        return json.loads((self.results / label / "state.json").read_text())["cells"][cell_id]

    def make_run(self, label="u", *extra):
        args = run_ci_local.argument_parser().parse_args(
            ["--hosts", str(self.hosts), "--remote-home", self.remote_home, "--results-dir", str(self.results),
             "run", "-l", label, "--src", str(self.src), *extra])
        run_ci_local.apply_settings(args, {})
        run = run_ci_local.Run(args, {})
        self.addCleanup(run.lock.close)
        return run

    def test_run_retry_report_and_cleanup(self):
        out, code = self.run_stress("x")
        self.assertEqual(3, code, out)
        state = json.loads((self.results / "x/state.json").read_text())
        cells = state["cells"]
        self.assertEqual({"jar_jdk11_amd64", STRESS, "report"}, set(cells))
        stress = cells[STRESS]
        self.assertEqual((0, "done", 2, "localhost"),
                         (stress["rc"], stress["status"], stress["attempt"], stress["host"]))
        cell_dir = self.results / "x/cells/stress-test_jdk11_python_3.8_no_amd64_1_1"
        self.assertEqual("4", (cell_dir / "attempt1/rc").read_text().strip())
        # organised as in the Jenkinsfile, and compressed
        self.assertTrue((cell_dir / "attempt2/output/stress-test/jdk_11/amd64"
                         / "TEST-org.apache.cassandra.StressTest.xml.xz").exists())
        self.assertTrue((cell_dir / "attempt2/stage.log.xz").exists())
        self.assertEqual("stress-test\tamd64\t11\t1\t1\t1",
                         "\t".join((cell_dir / "attempt2/cell-time.tsv").read_text().split("\t")[:6]))
        summary = (self.results / "x/ci_summary.html").read_text()
        self.assertIn("build/test/output/stress-test/jdk_11/amd64/TEST-org.apache.cassandra.StressTest.xml", summary)
        sha = subprocess.run(["git", "-C", str(self.src), "rev-parse", "HEAD"], stdout=subprocess.PIPE, text=True,
                             check=True).stdout.strip()
        # the build parameters, as Jenkins would have had them; this checkout has no upstream
        self.assertIn(f"tag=run-ci-local-x repository= branch={sha} profile=custom regexp=stress-test "
                      f"architecture=amd64 jdk=11 dtest=https://github.com/apache/cassandra-dtest trunk", summary)
        self.assertTrue((self.results / "x/generateTestReports.log.xz").exists())
        self.assertEqual([{"benchmark": "stress-test"}],
                         json.loads((self.results / "x/test/output/combined-result.json").read_text()))
        self.assertIn("Custom Profile Regexp: stress-test", out)
        self.assertIn("Fail Fast: True", out)
        self.assertIn("result: UNSTABLE", out)
        self.assertIn("tests: passed 1 | failed 1 | skipped 1", out)
        # the stashes are gone, here and on the host
        self.assertFalse((self.results / "x/stash").exists())
        self.assertFalse((self.tmp / "remote/runs/x").exists())

        # resuming runs nothing again
        out, code = self.run_stress("x", "--no-report")
        self.assertNotIn("STARTED", out)
        self.assertEqual(3, code)

    def test_jar_failure_skips_its_cells(self):
        write_executable(self.src / ".build/docker/build-jars.sh", '#!/bin/sh\nexit 1\n')
        self.commit("broken")
        out, code = self.run_stress("y", "--no-report")
        cells = json.loads((self.results / "y/state.json").read_text())["cells"]
        self.assertEqual((1, 2), (cells["jar_jdk11_amd64"]["rc"], cells["jar_jdk11_amd64"]["attempt"]))
        self.assertEqual("not run", cells[STRESS]["status"])
        self.assertIn("result: FAILURE", out)
        self.assertEqual(4, code)

    def test_test_cell_without_results_fails(self):
        write_executable(self.src / ".build/docker/run-tests.sh", '#!/bin/sh\nexit 0\n')
        self.commit("no results")
        out, code = self.run_stress("n", "--no-report")
        stress = self.cell_state("n", STRESS)
        self.assertEqual((0, 0, 2), (stress["rc"], stress["junit"], stress["attempt"]))
        self.assertIn("no test results", out)
        self.assertEqual(4, code)

    def test_dry_run_plans_from_the_commit(self):
        out, code = self.run_ci_local("run", "-l", "z", "--src", str(self.src), "-p", "packaging", "-n")
        self.assertIn("redhat jdk17", out)
        self.assertEqual(0, code)
        self.assertFalse((self.results / "z").exists())
        # an uncommitted edit does not change the plan
        jenkinsfile = self.src / ".jenkins/Jenkinsfile"
        jenkinsfile.write_text(jenkinsfile.read_text().replace("'packaging': [", "'packages': ["))
        out, _ = self.run_ci_local("run", "-l", "z", "--src", str(self.src), "-p", "packaging", "-n")
        self.assertIn("redhat jdk17", out)

    def test_a_changed_input_starts_a_new_generation(self):
        self.run_stress("g", "--no-report")
        with self.assertRaisesRegex(SystemExit, "build.properties or --maven-mirror"):
            self.run_stress("g", "--no-report", "--maven-mirror", "https://mirror.example/maven")
        (self.src / "README").write_text("x\n")
        self.commit("next")
        with self.assertRaisesRegex(SystemExit, "the commit changed"):
            self.run_stress("g", "--no-report")
        out, _ = self.run_stress("g", "--no-report", "--allow-sha-change")
        self.assertIn("STARTED jar jdk11", out)
        self.assertIn("STARTED stress-test jdk11", out)
        previous = list((self.results / "g/previous").iterdir())
        self.assertEqual(1, len(previous))
        self.assertTrue((previous[0] / "cells/stress-test_jdk11_python_3.8_no_amd64_1_1").is_dir())
        state = json.loads((self.results / "g/state.json").read_text())
        self.assertEqual(subprocess.run(["git", "-C", str(self.src), "rev-parse", "HEAD"], stdout=subprocess.PIPE,
                                        text=True, check=True).stdout.strip(), state["sha"])

    def test_another_drivers_label(self):
        owned = Path(self.remote_home) / "runs/o"
        (owned / "jobs").mkdir(parents=True)
        (owned / "owner").write_text("ffffffffffffffff\n")
        with self.assertRaisesRegex(SystemExit, "belongs to another driver"):
            self.run_stress("o", "--no-report")
        self.assertTrue(owned.exists())
        out, _ = self.run_ci_local("clean", "-l", "o")
        self.assertIn("belongs to another driver; left alone", out)
        self.assertTrue(owned.exists())
        out, _ = self.run_ci_local("clean", "-l", "o", "--force")
        self.assertFalse(owned.exists())

    def test_one_driver_per_label(self):
        lock = lock_label(self.results / "l")
        self.addCleanup(lock.close)
        with self.assertRaisesRegex(SystemExit, "another run-ci-local driver"):
            self.run_stress("l")

    def test_bad_labels(self):
        for label in ("..", ".x", "a/b", "-x", ""):
            with self.subTest(label=label), self.assertRaises(SystemExit):
                self.run_stress(label)
            with self.subTest(label=label), self.assertRaises(SystemExit):
                self.run_ci_local("clean", "-l", label)
        self.assertEqual("ok-1.x_y", check_name("ok-1.x_y"))

    def test_an_exception_while_following_a_cell_retries_it(self):
        real_fetch = run_ci_local.Run.fetch
        failed = []

        def fetch_failing_once(run, cell, host, attempt, attempt_id):
            if cell.kind == "test" and not failed:
                failed.append(cell.name)
                raise RuntimeError("connection reset")
            return real_fetch(run, cell, host, attempt, attempt_id)
        with patch.object(run_ci_local.Run, "fetch", fetch_failing_once):
            (self.tmp / "flaky").touch()
            out, code = self.run_stress("e", "--no-report")
        self.assertEqual(["stress-test jdk11"], failed)
        self.assertIn("RuntimeError: connection reset", out)
        stress = self.cell_state("e", STRESS)
        self.assertEqual(("done", 2), (stress["status"], stress["attempt"]))
        self.assertEqual(3, code)

    def test_a_host_that_fails_to_start_cells_is_skipped(self):
        self.hosts.write_text("broken 8\nlocalhost 8\n")
        self.remote_home = ".rcl"
        tmp = self.tmp
        real_try_start = run_ci_local.Run.try_start

        def command(host, remote_command):
            return ["env", f"HOME={tmp}/home-{host.name}", "bash", "-c", remote_command]

        def try_start(run, cell, host, attempt_id, reserve=False):
            if host.name == "broken":
                raise HostError("broken: ssh: connect to host broken port 22: Connection refused")
            return real_try_start(run, cell, host, attempt_id, reserve)
        (self.tmp / "flaky").touch()
        with patch.object(run_ci_local.Host, "command", command), \
                patch.object(run_ci_local.Run, "try_start", try_start):
            out, code = self.run_stress("b", "--no-report")
        self.assertIn("cannot start on broken", out)
        cells = json.loads((self.results / "b/state.json").read_text())["cells"]
        self.assertEqual({"localhost"}, {c["host"] for c in cells.values()})
        self.assertEqual(3, code)

    def test_cells_go_to_hosts_large_enough(self):
        self.hosts.write_text("small 2\nlocalhost 8\n")
        self.remote_home = ".rcl"
        tmp = self.tmp

        def command(host, remote_command):
            return ["env", f"HOME={tmp}/home-{host.name}", "bash", "-c", remote_command]
        with patch.object(run_ci_local.Host, "command", command):
            out, code = self.run_ci_local("run", "-l", "s", "--src", str(self.src), "-p", "custom", "-e", "lint",
                                          "-j", "11", "--no-report")
        cells = json.loads((self.results / "s/state.json").read_text())["cells"]
        self.assertEqual("localhost", cells["lint_jdk11_amd64"]["host"], out)
        self.assertEqual(0, code)

    def test_wait_gives_up_on_an_unreachable_host(self):
        run = self.make_run()

        def unreachable(_):
            raise HostError("fake: ssh timed out")
        cell = Cell('test', {'type': 'test', 'splits': 1, 'timeout_hours': 1}, 3, '11', '3.8', 'no')
        with patch("run_ci_local.time.sleep"), patch("run_ci_local.SSH_FAILURES_LOST", 2):
            self.assertIsNone(run.wait(cell, FakeHost(unreachable), "1-a", time.time() + 3600))

    def test_wait_stops_a_cell_past_its_deadline(self):
        run = self.make_run()
        host = FakeHost(lambda command: subprocess.CompletedProcess(command, 0, b"running\n", b""))
        cell = Cell('test', {'type': 'test', 'splits': 1, 'timeout_hours': 1}, 3, '11', '3.8', 'no')
        with patch("run_ci_local.time.sleep"):
            self.assertIsNone(run.wait(cell, host, "1-a", time.time() - 1))
        self.assertTrue(any(" stop " in c and "1-a" in c for c in host.commands), host.commands)

    def test_an_unreadable_result_is_lost(self):
        run = self.make_run()

        def truncated(command, stdout=None, **_):
            if "result.tar" in command:
                stdout.write(b"not a tar file")
            return subprocess.CompletedProcess(command, 0, b"", b"")
        host = FakeHost(None)
        host.run = truncated
        cell = Cell('test', {'type': 'test', 'splits': 1, 'timeout_hours': 1}, 3, '11', '3.8', 'no')
        self.assertFalse(run.fetch(cell, host, 1, "1-a"))

    def test_parameter_precedence(self):
        def profile(*args):
            out, _ = self.run_ci_local("run", "-l", "p", "--src", str(self.src), "-n", *args)
            return re.search(r"Profile: (.*)", out).group(1), re.search(r"Fail Fast: (.*)", out).group(1)
        # the Jenkinsfile's defaults
        self.assertEqual(("skinny", "True"), profile())
        # the config file over them
        self.config.write_text("[run-ci-local]\nprofile = packaging\nfail_fast = no\n")
        self.assertEqual(("packaging", "False"), profile())
        # the command line over both
        self.assertEqual(("pre-commit", "True"), profile("-p", "pre-commit", "--fail-fast"))
        # --config over $RUN_CI_LOCAL_CONFIG
        other = self.tmp / "other.conf"
        other.write_text("[run-ci-local]\nprofile = skinny\nresults_dir = " + str(self.tmp / "elsewhere") + "\n")
        out, _ = self.run_ci_local("--config", str(other), "run", "-l", "p", "--src", str(self.src), "-n")
        self.assertIn("Profile: skinny", out)

    def test_settings_from_the_config_file(self):
        self.config.write_text(f"[run-ci-local]\nresults_dir = {self.tmp / 'configured'}\nreport = no\n")
        real_sleep = time.sleep
        with patch("run_ci_local.time.sleep", lambda s: real_sleep(min(s, 0.2))), \
                contextlib.redirect_stdout(io.StringIO()):
            run_ci_local.main(["--hosts", str(self.hosts), "--remote-home", self.remote_home, "run", "-l", "c",
                               "--src", str(self.src), "-p", "custom", "-e", "stress-test", "-j", "11"])
        self.assertTrue((self.tmp / "configured/c/state.json").exists())
        self.assertFalse((self.tmp / "configured/c/ci_summary.html").exists())

    def test_a_resumed_label_keeps_its_parameters(self):
        (self.tmp / "flaky").touch()
        self.run_stress("r", "--no-report")
        stored = json.loads((self.results / "r/state.json").read_text())["parameters"]
        self.assertEqual(("custom", "stress-test", "11", None), (stored["profile"], stored["profile_custom_regexp"],
                                                                 stored["jdk"], stored["repository"]))
        # the config file does not change a resumed label
        self.config.write_text("[run-ci-local]\nprofile = packaging\n")
        out, _ = self.run_ci_local("run", "-l", "r", "--src", str(self.src), "--no-report")
        self.assertIn("Profile: custom", out)
        self.assertNotIn("STARTED", out)
        with self.assertRaisesRegex(SystemExit, "ran with profile 'custom'; use --force"):
            self.run_ci_local("run", "-l", "r", "--src", str(self.src), "-p", "packaging", "-n")
        out, _ = self.run_ci_local("run", "-l", "r", "--src", str(self.src), "-p", "packaging", "--force", "-n")
        self.assertIn("Profile: packaging", out)
        # fail fast may change on a resumed label
        out, _ = self.run_ci_local("run", "-l", "r", "--src", str(self.src), "--no-fail-fast", "-n")
        self.assertIn("Fail Fast: False", out)

    def fail_one_cell(self):
        # stress-test fails, fqltool-test takes a while
        write_executable(self.src / ".build/docker/run-tests.sh", f"""#!/bin/bash
case "$2" in
  stress-test) exit 4 ;;
  fqltool-test) [ -f "{self.tmp}/fast" ] || sleep 30 ;;
esac
mkdir -p build/test/output/$2
echo '<testsuite><testcase classname="Fql" name="test"/></testsuite>' > build/test/output/$2/TEST-Fql.xml
""")
        self.commit("one failing cell")

    def test_fail_fast(self):
        self.fail_one_cell()
        out, code = self.run_ci_local("run", "-l", "f", "--src", str(self.src), "-p", "custom", "-e",
                                      "stress-test|fqltool-test", "-j", "11")
        cells = json.loads((self.results / "f/state.json").read_text())["cells"]
        self.assertEqual(2, cells[STRESS]["attempt"])
        self.assertEqual("aborted", cells["fqltool-test_jdk11_python_3.8_no_amd64_1_1"]["status"])
        self.assertIn("FAIL FAST: stress-test jdk11 failed", out)
        self.assertFalse((self.results / "f/ci_summary.html").exists())
        self.assertEqual(4, code)

    def test_no_fail_fast(self):
        self.fail_one_cell()
        (self.tmp / "fast").touch()
        out, code = self.run_ci_local("run", "-l", "g", "--src", str(self.src), "-p", "custom", "-e",
                                      "stress-test|fqltool-test", "-j", "11", "--no-fail-fast")
        cells = json.loads((self.results / "g/state.json").read_text())["cells"]
        self.assertEqual("done", cells["fqltool-test_jdk11_python_3.8_no_amd64_1_1"]["status"])
        self.assertTrue((self.results / "g/ci_summary.html").exists())
        self.assertNotIn("FAIL FAST", out)
        self.assertEqual(4, code)

    def test_cells_go_to_hosts_of_their_architecture(self):
        self.hosts.write_text("localhost 8 arch=arm64\n")
        out, _ = self.run_ci_local("run", "-l", "a", "--src", str(self.src), "-p", "custom", "-e", "lint",
                                   "-j", "11", "--architecture", "arm64", "--no-report")
        cells = json.loads((self.results / "a/state.json").read_text())["cells"]
        self.assertEqual({"jar_jdk11_arm64", "lint_jdk11_arm64"}, set(cells))
        self.assertIn("Architecture: arm64", out)
        with self.assertRaisesRegex(SystemExit, "no host for the amd64 cells"):
            self.run_ci_local("run", "-l", "b", "--src", str(self.src), "-p", "custom", "-e", "lint", "-j", "11",
                              "--architecture", "all", "--no-report")

    def test_a_branch_of_a_repository(self):
        # as Jenkins does: fetch the branch, test it, and report it
        upstream = self.tmp / "upstream.git"
        subprocess.run(["git", "clone", "-q", "--bare", str(self.src), str(upstream)], check=True)
        (self.src / "local-only").write_text("x\n")
        self.commit("not pushed")
        repository = f"file://{upstream}"
        (self.tmp / "flaky").touch()
        out, code = self.run_stress("rb", "-r", repository, "-b", "trunk")
        self.assertIn(f"Repository: {repository}", out)
        self.assertIn("Branch: trunk", out)
        state = json.loads((self.results / "rb/state.json").read_text())
        upstream_sha = subprocess.run(["git", "-C", str(upstream), "rev-parse", "trunk"], stdout=subprocess.PIPE,
                                      text=True, check=True).stdout.strip()
        self.assertEqual(upstream_sha, state["sha"])
        self.assertEqual((repository, "trunk"), (state["parameters"]["repository"], state["parameters"]["branch"]))
        summary = (self.results / "rb/ci_summary.html").read_text()
        self.assertIn(f"repository={repository} branch=trunk", summary)
        self.assertEqual(3, code)

    def test_another_driver_taking_the_label_ends_the_run(self):
        def taken(run, cell, host, attempt_id, reserve=False):
            raise run_ci_local.OwnerError(f"{host}: label {run.label} now belongs to another driver; stopping")
        with patch.object(run_ci_local.Run, "try_start", taken), \
                self.assertRaisesRegex(SystemExit, "now belongs to another driver"):
            self.run_stress("t", "--no-report")

    def test_the_label_lock_comes_first(self):
        out = self.results / "k"
        out.mkdir(parents=True)
        (out / "state.json").write_text("not json")
        lock = lock_label(out)
        self.addCleanup(lock.close)
        with self.assertRaisesRegex(SystemExit, "another run-ci-local driver"):
            self.run_stress("k")

    def test_a_dry_run_fetches_into_a_temporary_checkout(self):
        upstream = self.tmp / "upstream.git"
        subprocess.run(["git", "clone", "-q", "--bare", str(self.src), str(upstream)], check=True)
        out, code = self.run_ci_local("run", "-l", "d", "--src", str(self.src), "-r", f"file://{upstream}",
                                      "-b", "trunk", "-p", "packaging", "-n")
        self.assertEqual(0, code)
        self.assertIn("Branch: trunk", out)
        self.assertFalse((self.results / "d").exists())

    def make_dtest_repository(self):
        dtest = self.tmp / "cassandra-dtest"
        dtest.mkdir()
        (dtest / "requirements.txt").write_text("pytest\n")
        for args in (["init", "-q", "-b", "trunk"], ["add", "."],
                     ["-c", "user.name=t", "-c", "user.email=t@example.com", "commit", "-q", "-m", "dtest"]):
            subprocess.run(["git", "-C", str(dtest), *args], check=True)
        return dtest

    def dtest_head(self, dtest):
        return subprocess.run(["git", "-C", str(dtest), "rev-parse", "HEAD"], stdout=subprocess.PIPE, text=True,
                              check=True).stdout.strip()

    def test_a_label_keeps_its_dtest_commit(self):
        dtest = self.make_dtest_repository()
        first = self.dtest_head(dtest)
        (self.tmp / "flaky").touch()
        args = ("run", "-l", "dt", "--src", str(self.src), "-p", "custom", "-e", "dtest", "--cells",
                "^dtest jdk11 1/64$", "-j", "11", "-d", f"file://{dtest}", "-k", "trunk", "--no-report")
        self.run_ci_local(*args)
        self.assertEqual(first, json.loads((self.results / "dt/state.json").read_text())["dtest_commit"])
        (dtest / "new_test.py").write_text("\n")
        subprocess.run(["git", "-C", str(dtest), "add", "."], check=True)
        subprocess.run(["git", "-C", str(dtest), "-c", "user.name=t", "-c", "user.email=t@example.com",
                        "commit", "-q", "-m", "more"], check=True)
        self.run_ci_local(*args)
        self.assertEqual(first, json.loads((self.results / "dt/state.json").read_text())["dtest_commit"])
        out, _ = self.run_ci_local(*args, "--force")
        state = json.loads((self.results / "dt/state.json").read_text())
        self.assertEqual(self.dtest_head(dtest), state["dtest_commit"])
        self.assertIn("STARTED dtest jdk11 1/64", out)

    def test_the_summary_is_of_this_runs_cells(self):
        (self.tmp / "flaky").touch()
        self.run_stress("sc", "--no-report")
        out, _ = self.run_ci_local("run", "-l", "sc", "--src", str(self.src), "-p", "custom", "-e", "fqltool-test",
                                   "-j", "11", "--no-report", "--force")
        summary = out[out.index("run-ci-local sc:"):]
        self.assertIn("fqltool-test jdk11", summary)
        self.assertNotIn("stress-test jdk11", summary)

    def test_a_cell_no_host_can_start_fails(self):
        def broken(run, cell, host, attempt_id, reserve=False):
            raise HostError(f"{host}: ssh: connect to host port 22: Connection refused")
        with patch.object(run_ci_local.Run, "try_start", broken), patch("run_ci_local.START_FAILURE_LIMIT", 2):
            out, code = self.run_stress("u", "--no-report")
        cells = json.loads((self.results / "u/state.json").read_text())["cells"]
        self.assertEqual("unstartable", cells["jar_jdk11_amd64"]["status"])
        self.assertEqual("not run", cells[STRESS]["status"])
        self.assertIn("UNSTARTABLE jar jdk11", out)
        self.assertEqual(4, code)

    def slow_jdk17_jar(self):
        write_executable(self.src / ".build/docker/build-jars.sh",
                         '#!/bin/sh\n[ "$1" = 17 ] && sleep 3\nmkdir -p build && echo jar > build/jar.txt\n')
        self.commit("slow jar")
        (self.tmp / "flaky").touch()

    def test_fail_fast_waits_for_every_jar_cell(self):
        self.slow_jdk17_jar()
        out, _ = self.run_ci_local("run", "-l", "w", "--src", str(self.src), "-p", "custom", "-e", "stress-test",
                                   "--no-report")
        self.assertLess(out.index("FINISHED jar jdk17"), out.index("STARTED stress-test jdk11"))

    def test_no_fail_fast_starts_cells_once_their_jar_is_built(self):
        self.slow_jdk17_jar()
        out, _ = self.run_ci_local("run", "-l", "nw", "--src", str(self.src), "-p", "custom", "-e", "stress-test",
                                   "--no-report", "--no-fail-fast")
        self.assertLess(out.index("STARTED stress-test jdk11"), out.index("FINISHED jar jdk17"))

    def test_clean_keeps_the_local_stash_of_a_label_a_host_kept(self):
        owned = Path(self.remote_home) / "runs/kept"
        (owned / "jobs").mkdir(parents=True)
        (owned / "owner").write_text(run_ci_local.owner_id(self.results.resolve()) + "\n")
        # one of its cells still runs there
        runner = fake_runner("kept", "c")
        self.addCleanup(runner.kill)
        (owned / "jobs/c.pid").write_text(f"{runner.pid}\n")
        (self.results / "kept/stash").mkdir(parents=True)
        (self.results / "kept" / run_ci_local.LABEL_MARKER).touch()
        out, _ = self.run_ci_local("clean", "-l", "kept")
        self.assertTrue((self.results / "kept/stash").exists())
        self.assertIn("local stash of kept is kept", out)

    def test_set_capacity(self):
        self.hosts.write_text("localhost 3\n")
        self.run_ci_local("status")
        (Path(self.remote_home) / "capacity").write_text("5\n")
        self.run_ci_local("status")
        self.assertEqual("5", (Path(self.remote_home) / "capacity").read_text().strip())
        self.run_ci_local("set-capacity")
        self.assertEqual("3", (Path(self.remote_home) / "capacity").read_text().strip())

    def test_a_remote_home_with_whitespace_is_refused(self):
        self.remote_home = "ci home"
        with self.assertRaisesRegex(SystemExit, "without whitespace"):
            self.run_ci_local("status")

    def test_a_failing_test_report_still_makes_the_summary(self):
        write_executable(self.src / ".build/docker/_docker_run.sh", """#!/bin/bash
[ "$2" == "ci/generate-test-report.sh" ] && exit 1
if [ "$2" == "ci/generate-ci-summary.sh" ] ; then
  echo "<html>summary</html>" > build/ci_summary.html
fi
""")
        self.commit("failing report")
        (self.tmp / "flaky").touch()
        self.run_stress("rp")
        self.assertEqual("<html>summary</html>\n", (self.results / "rp/ci_summary.html").read_text())
        log = lzma.open(self.results / "rp/generateTestReports.log.xz", "rt").read()
        self.assertIn("failed generate-test-report.sh of stress-test", log)

    def test_force_does_not_take_another_drivers_label(self):
        owned = Path(self.remote_home) / "runs/o2"
        (owned / "jobs").mkdir(parents=True)
        (owned / "owner").write_text("ffffffffffffffff\n")
        with self.assertRaisesRegex(SystemExit, "belongs to another driver.*use another label"):
            self.run_stress("o2", "--no-report", "--force")
        self.assertEqual("ffffffffffffffff", (owned / "owner").read_text().strip())

    def test_force_keeps_the_parameters_not_given(self):
        (self.tmp / "flaky").touch()
        self.run_stress("fk", "--no-report")
        out, _ = self.run_ci_local("run", "-l", "fk", "--src", str(self.src), "--force", "--no-report")
        self.assertIn("Profile: custom", out)
        self.assertIn("Custom Profile Regexp: stress-test", out)
        self.assertIn("JDK: 11", out)
        self.assertIn("STARTED stress-test jdk11", out)

    def test_a_base_is_the_one_its_jar_attempt_built(self):
        (self.tmp / "flaky").touch()
        self.run_stress("bv", "--no-report", "--keep-stashes")
        state = json.loads((self.results / "bv/state.json").read_text())
        first = state["stashes"]["base-amd64-jdk11.tar"]
        self.assertEqual(state["cells"]["jar_jdk11_amd64"]["attempt_id"], first.split("-", 1)[1])
        self.run_stress("bv", "--no-report", "--force")
        second = json.loads((self.results / "bv/state.json").read_text())["stashes"]["base-amd64-jdk11.tar"]
        self.assertNotEqual(first, second)

    def test_unreachable_hosts_are_waited_for(self):
        def nobody(run):
            return {h.name: (0, 0) for h in run.hosts}
        with patch.object(run_ci_local.Run, "host_free", nobody), \
                patch("run_ci_local.HOSTS_UNREACHABLE_SECONDS", 1):
            out, code = self.run_stress("ur", "--no-report")
        self.assertIn("UNSTARTABLE jar jdk11: no host of its architecture has answered", out)
        self.assertEqual(4, code)
        # a resumed label tries an unstartable cell again
        (self.tmp / "flaky").touch()
        out, code = self.run_stress("ur", "--no-report")
        self.assertIn("STARTED jar jdk11", out)
        self.assertEqual(3, code)

    def test_a_resumed_label_whose_jar_failed_stops_with_fail_fast(self):
        write_executable(self.src / ".build/docker/build-jars.sh",
                         '#!/bin/sh\n[ "$1" = 17 ] && exit 1\nmkdir -p build && echo jar > build/jar.txt\n')
        self.commit("jdk17 jar fails")
        (self.tmp / "flaky").touch()
        self.run_ci_local("run", "-l", "jf", "--src", str(self.src), "-p", "custom", "-e", "stress-test", "--no-report",
                          "--no-fail-fast")
        out, code = self.run_ci_local("run", "-l", "jf", "--src", str(self.src), "--no-report", "--fail-fast")
        self.assertIn("FAIL FAST: jar jdk17 failed", out)
        self.assertEqual(4, code)

    def test_a_report_is_of_this_run(self):
        out_dir = self.results / "st"
        out_dir.mkdir(parents=True)
        for name in ("ci_summary.html", "results_details.tar.xz", "generateTestReports.log.xz"):
            (out_dir / name).write_text("an earlier run's\n")
        (self.tmp / "flaky").touch()
        out, _ = self.run_stress("st", "--no-report")
        for name in ("ci_summary.html", "results_details.tar.xz", "generateTestReports.log.xz"):
            self.assertFalse((out_dir / name).exists(), name)
        self.assertNotIn("for a JIRA ticket", out)

    def test_a_report_without_its_files_fails(self):
        write_executable(self.src / ".build/docker/_docker_run.sh", "#!/bin/bash\nexit 0\n")
        self.commit("no report files")
        (self.tmp / "flaky").touch()
        out, code = self.run_stress("nr")
        report = json.loads((self.results / "nr/state.json").read_text())["cells"]["report"]
        self.assertEqual((0, False), (report["rc"], report["artifacts"]))
        self.assertIn("no report", out)
        self.assertNotIn("for a JIRA ticket", out)
        self.assertEqual(4, code)

    def test_clean_holds_the_label_lock(self):
        (self.results / "held").mkdir(parents=True)
        (self.results / "held" / run_ci_local.LABEL_MARKER).touch()
        lock = lock_label(self.results / "held")
        self.addCleanup(lock.close)
        out, _ = self.run_ci_local("clean", "-l", "held")
        self.assertIn("held is running on this machine; kept", out)

    def test_java_named_groups_and_regex_errors(self):
        out, _ = self.run_ci_local("run", "-l", "rx", "--src", str(self.src), "-p", "custom", "-e",
                                   "(?<step>stress-test)", "-j", "11", "-n")
        self.assertIn("stress-test jdk11", out)
        with self.assertRaisesRegex(SystemExit, "Python regex"):
            self.run_ci_local("run", "-l", "rx", "--src", str(self.src), "-p", "custom", "-e", "(stress", "-n")

    def test_a_host_with_its_own_home(self):
        home = self.tmp / "elsewhere"
        self.hosts.write_text(f"localhost 8 home={home}\n")
        (self.tmp / "flaky").touch()
        self.run_stress("hh", "--no-report", "--keep-stashes")
        self.assertTrue((home / "runs/hh/src.tar").exists())
        self.assertFalse((Path(self.remote_home) / "runs").exists())
        self.run_ci_local("clean", "-l", "hh")
        self.assertFalse((home / "runs/hh").exists())

    def test_a_finished_attempt_is_read_past_its_deadline(self):
        run = self.make_run()
        host = FakeHost(lambda command: subprocess.CompletedProcess(command, 0, b"done 0\n", b""))
        cell = Cell('test', {'type': 'test', 'splits': 1, 'timeout_hours': 1}, 3, '11', '3.8', 'no')
        with patch("run_ci_local.time.sleep"):
            self.assertEqual(0, run.wait(cell, host, "1-a", time.time() - 3600))
        self.assertFalse(any(" stop " in c for c in host.commands), host.commands)

    def test_a_result_is_kept_before_the_host_forgets_it(self):
        run = self.make_run()
        cell = Cell('jar', {'type': 'build', 'splits': 1, 'timeout_hours': 1, 'script': 'build-jars.sh'}, 1, '11',
                    '3.8', 'no')
        saved = []

        def answer(command):
            if " clean " in command:
                saved.append(json.loads(run.state_file.read_text())["cells"][cell.cell_id]["status"])
            return subprocess.CompletedProcess(command, 0, b"done 0\n", b"")
        with patch("run_ci_local.time.sleep"), patch.object(run_ci_local.Run, "fetch", lambda *a: True), \
                contextlib.redirect_stdout(io.StringIO()):
            run.run_cell(cell, FakeHost(answer), 1, "1-a", time.time() + 3600)
        self.assertEqual(["done"], saved)

    def test_a_failing_summary_fails_the_report(self):
        write_executable(self.src / ".build/docker/_docker_run.sh", """#!/bin/bash
if [ "$2" == "ci/generate-ci-summary.sh" ] ; then
  echo "<html>half</html>" > build/ci_summary.html
  exit 1
fi
""")
        self.commit("summary fails")
        (self.tmp / "flaky").touch()
        out, code = self.run_stress("sf")
        report = json.loads((self.results / "sf/state.json").read_text())["cells"]["report"]
        self.assertNotEqual(0, report["rc"])
        self.assertFalse((self.results / "sf/ci_summary.html").exists())
        self.assertEqual(4, code)

    def test_no_start_after_fail_fast_stopped_the_run(self):
        run = self.make_run()
        run.state["sha"], run.state["stashes"]["src.tar"] = run.sha, run.inputs
        run.aborting = "jar jdk11"
        host = FakeHost(lambda command: subprocess.CompletedProcess(command, 0, b"x\n", b""))
        run.uploaded[(host.name, "src.tar")] = run.inputs
        cell = Cell('lint', {'type': 'build', 'splits': 1, 'timeout_hours': 1, 'script': 'check-code.sh'}, 3, '11',
                    '3.8', 'no')
        cell.kind = "jar"
        self.assertFalse(run.try_start(cell, host, "1-a"))
        self.assertFalse(any(" start " in c for c in host.commands), host.commands)

    def test_the_summary_counts_planned_cells_that_did_not_run(self):
        out = self.tmp / "planned"
        out.mkdir()
        (out / "state.json").write_text(json.dumps({"plan": {"a": {"name": "jar jdk11", "kind": "jar"},
                                                             "b": {"name": "test jdk11 1/2", "kind": "test"}},
                                                    "cells": {"a": {"name": "jar jdk11", "status": "done", "rc": 0,
                                                                    "kind": "jar"}}}))
        text, result = summarise(out)
        self.assertEqual("FAILURE", result)
        self.assertIn("test jdk11 1/2: not run", text)

    def test_a_build_only_run_needs_no_report(self):
        out, code = self.run_ci_local("run", "-l", "bo", "--src", str(self.src), "-p", "custom", "-e", "lint",
                                      "-j", "11")
        state = json.loads((self.results / "bo/state.json").read_text())
        self.assertFalse(state["report"])
        self.assertNotIn("report", state["plan"])
        self.assertEqual(0, code)

    def test_a_host_that_does_not_answer_at_the_start_is_skipped(self):
        self.hosts.write_text("away 8\nlocalhost 8\n")
        real_command = run_ci_local.Host.command

        def command(host, remote_command):
            return ["false"] if host.name == "away" else real_command(host, remote_command)
        (self.tmp / "flaky").touch()
        with patch.object(run_ci_local.Host, "command", command):
            out, code = self.run_stress("sk", "--no-report")
        self.assertIn("WARNING: away cannot be set up yet, and is tried again in 20s", out)
        self.assertIn("removed sk on localhost; the local stash is kept, as away still has the label", out)
        self.assertTrue((self.results / "sk/stash").is_dir())
        self.assertEqual(3, code)

    def test_a_host_that_fails_its_checks_is_left_out(self):
        self.hosts.write_text("rooted 8\nlocalhost 8\n")
        root_bin = self.tmp / "root-bin"
        write_executable(root_bin / "id", '#!/bin/sh\n[ "$1" = "-u" ] && echo 0\n')
        tmp, real_command = self.tmp, run_ci_local.Host.command

        def command(host, remote_command):
            if host.name == "rooted":
                return ["env", f"PATH={root_bin}:{os.environ['PATH']}", f"HOME={tmp}/home-rooted", "bash", "-c",
                        remote_command]
            return real_command(host, remote_command)
        (self.tmp / "flaky").touch()
        with patch.object(run_ci_local.Host, "command", command):
            out, code = self.run_stress("lo2", "--no-report")
        self.assertIn("rooted cannot run cells: runs as root", out)
        self.assertIn("it is left out", out)
        self.assertEqual(3, code)
        self.hosts.write_text("rooted 8\n")
        with patch.object(run_ci_local.Host, "command", command), \
                self.assertRaisesRegex(SystemExit, "no host can run cells: rooted cannot run cells"):
            self.run_stress("lo3", "--no-report")

    def test_status_and_clean_start_no_container(self):
        self.run_ci_local("status")
        self.run_ci_local("clean", "-l", "nothing")
        calls = self.runtime.calls.read_text()
        self.assertNotIn(" run ", calls)
        self.assertFalse((self.results / "nothing").exists())

    def test_clean_removes_only_a_stash_with_the_marker(self):
        (self.results / "foreign/stash").mkdir(parents=True)
        (self.results / "foreign/state.json").write_text("{}")
        self.run_ci_local("clean", "-l", "foreign")
        self.assertTrue((self.results / "foreign/stash").exists())
        (self.results / "foreign" / run_ci_local.LABEL_MARKER).touch()
        self.run_ci_local("clean", "-l", "foreign")
        self.assertFalse((self.results / "foreign/stash").exists())

    def test_a_label_gets_its_marker(self):
        (self.tmp / "flaky").touch()
        self.run_stress("mk", "--no-report")
        self.assertTrue((self.results / "mk" / run_ci_local.LABEL_MARKER).is_file())

    def test_inputs_can_change_back_and_forth(self):
        (self.tmp / "flaky").touch()
        mirrors = ("", "https://mirror.example/a", "", "https://mirror.example/a")
        for mirror in mirrors:
            self.run_stress("ab2", "--no-report", "--allow-sha-change", *(["--maven-mirror", mirror] if mirror else []))
        previous = sorted(p.name.split("-", 1)[0] for p in (self.results / "ab2/previous").iterdir())
        self.assertEqual(["1", "2", "3"], previous)

    def test_clean_removes_nothing_while_a_label_runs_or_a_host_cannot_be_asked(self):
        (self.results / "cl/stash").mkdir(parents=True)
        (self.results / "cl" / run_ci_local.LABEL_MARKER).touch()

        def listing(live):
            return lambda command: subprocess.CompletedProcess(command, 0, f"cl {live} 0 0 {OWNER}\n".encode(), b"")

        def unreachable(_):
            raise HostError("away: ssh: connect to host away port 22: Connection timed out")
        for other, why in ((FakeHost(listing(1), "busy"), "busy: cl still runs a cell"),
                           (FakeHost(unreachable, "away"), "a host could not be asked")):
            idle = FakeHost(listing(0), "idle")
            with self.subTest(other=other), contextlib.redirect_stdout(io.StringIO()) as out:
                run_ci_local.sweep([idle, other], OWNER, labels={"cl"}, results_dir=self.results)
            self.assertIn(why, out.getvalue())
            self.assertFalse(any(" rmlabel " in c for c in idle.commands), idle.commands)
            self.assertTrue((self.results / "cl/stash").is_dir())
        hosts = [FakeHost(listing(0), "idle"), FakeHost(listing(0), "idle2")]
        with contextlib.redirect_stdout(io.StringIO()):
            run_ci_local.sweep(hosts, OWNER, labels={"cl"}, results_dir=self.results)
        self.assertTrue(all(any(" rmlabel cl " in c for c in h.commands) for h in hosts))
        self.assertFalse((self.results / "cl/stash").exists())

    def test_clean_holds_the_label_lock_to_the_end(self):
        (self.results / "lk").mkdir(parents=True)
        (self.results / "lk" / run_ci_local.LABEL_MARKER).touch()
        held = []

        def sweep(*_, **__):
            lock = lock_label(self.results / "lk")
            held.append(lock is None)
            if lock is not None:
                lock.close()
        with patch.object(run_ci_local, "sweep", sweep):
            self.run_ci_local("clean", "-l", "lk")
        self.assertEqual([True], held)

    def test_clean_reaches_the_hosts_a_label_ran_on(self):
        old_home = self.tmp / "old-home"
        owner = run_ci_local.owner_id(self.results.resolve())
        (old_home / "runs/gl/jobs").mkdir(parents=True)
        (old_home / "runs/gl/owner").write_text(owner + "\n")
        (self.results / "gl").mkdir(parents=True)
        (self.results / "gl" / run_ci_local.LABEL_MARKER).touch()
        (self.results / "gl/state.json").write_text(json.dumps({"cells": {"c": {
            "name": "c", "status": "done", "rc": 0, "host": "gone", "home": str(old_home)}}}))
        real_command = run_ci_local.Host.command

        def command(host, remote_command):
            return ["bash", "-c", remote_command] if host.name == "gone" else real_command(host, remote_command)
        with patch.object(run_ci_local.Host, "command", command):
            self.run_ci_local("clean", "-l", "gl")
        self.assertFalse((old_home / "runs/gl").exists())

    def test_a_failed_clean_up_keeps_the_result(self):
        cell = Cell('jar', {'type': 'build', 'splits': 1, 'timeout_hours': 1, 'script': 'build-jars.sh'}, 1, '11',
                    '3.8', 'no')

        def unreachable(command):
            raise HostError("fake: Connection reset by peer")
        for clean, said in ((unreachable, "cleaning up jar jdk11 on fake: fake: Connection reset by peer"),
                            (lambda command: subprocess.CompletedProcess(command, 1, b"", b"rm: cannot remove"),
                             "cleaning up jar jdk11 on fake failed with 1: rm: cannot remove")):
            run = self.make_run(f"c{len(said)}")

            def answer(command, clean=clean):
                if " clean " in command:
                    return clean(command)
                return subprocess.CompletedProcess(command, 0, b"done 0\n", b"")
            with self.subTest(said=said), patch("run_ci_local.time.sleep"), \
                    patch.object(run_ci_local.Run, "fetch", lambda *a: True), \
                    contextlib.redirect_stdout(io.StringIO()) as out:
                self.assertTrue(run.run_cell(cell, FakeHost(answer), 1, "1-a", time.time() + 3600))
            self.assertIn(said, out.getvalue())
            entry = json.loads(run.state_file.read_text())["cells"][cell.cell_id]
            self.assertEqual(("done", 0, 1), (entry["status"], entry["rc"], entry["done_attempts"]))
    def test_an_attempt_is_found_where_it_started(self):
        hosts = load_hosts(str(self.hosts), self.remote_home)
        record = {"host": "localhost", "home": "/elsewhere", "version": "v0", "runtime": "podman"}
        host = run_ci_local.attempt_host(hosts, record, self.remote_home)
        self.assertEqual(("/elsewhere", "v0", "podman"), (host.remote.given_home, host.remote.version, host.runtime))
        same = dict(record, home=self.remote_home, version=run_ci_local.remote_version())
        self.assertIs(hosts[0], run_ci_local.attempt_host(hosts, same, self.remote_home))
        gone = run_ci_local.attempt_host(hosts, {"host": "gone", "attempt_id": "1-a"}, self.remote_home)
        self.assertEqual(("gone", self.remote_home), (gone.name, gone.remote.given_home))

    def test_status_asks_an_attempt_where_it_started(self):
        old_home = self.tmp / "old-home"
        shutil.copytree(JOB_SH.parent, old_home / "bin/v0")
        (old_home / "runs/sa/jobs").mkdir(parents=True)
        (old_home / "runs/sa/owner").write_text(OWNER + "\n")
        (old_home / "runs/sa/jobs/c.attempt").write_text("1-a\n")
        (old_home / "runs/sa/jobs/c.rc").write_text("0\n")
        (self.results / "sa").mkdir(parents=True)
        (self.results / "sa/state.json").write_text(json.dumps({"cells": {"c": {
            "name": "c", "status": "running", "host": "gone", "home": str(old_home), "version": "v0",
            "attempt_id": "1-a", "attempt": 1}}}))
        real_command = run_ci_local.Host.command

        def command(host, remote_command):
            return ["bash", "-c", remote_command] if host.name == "gone" else real_command(host, remote_command)
        with patch.object(run_ci_local.Host, "command", command):
            out, _ = self.run_ci_local("status", "-l", "sa")
        self.assertIn("running: c on gone: done 0", out)

    def test_the_report_and_jmh_take_the_recorded_attempts(self):
        run = self.make_run()
        run.state.update(plan={"a": {"name": "a", "kind": "test"}, "b": {"name": "b", "kind": "test"}},
                         cells={"a": {"name": "a", "status": "done", "rc": 0, "attempt": 2},
                                "b": {"name": "b", "status": "not run", "rc": None, "attempt": 1}})
        for cell_id, attempt in (("a", 1), ("a", 2), ("b", 1)):
            output = run.out / "cells" / cell_id / f"attempt{attempt}/output/step"
            output.mkdir(parents=True)
            (output / "jmh-result.json").write_text(json.dumps([{"benchmark": f"{cell_id}{attempt}"}]))
        run.stash.mkdir(parents=True)
        with patch.object(run_ci_local.Run, "schedule"), contextlib.redirect_stdout(io.StringIO()):
            run.combine_jmh_results()
            run.report()
        self.assertEqual([{"benchmark": "a2"}], json.loads((run.out / "test/output/combined-result.json").read_text()))
        with tarfile.open(run.stash / "report-input.tar") as tar:
            self.assertEqual(["test/output/step/jmh-result.json"], tar.getnames())
            self.assertIn(b"a2", tar.extractfile("test/output/step/jmh-result.json").read())

    def test_a_host_another_driver_claimed_is_left_out_during_the_run(self):
        self.hosts.write_text("late 8\nlocalhost 8\n")
        self.remote_home = ".rcl"
        tmp, real_command = self.tmp, run_ci_local.Host.command
        late = tmp / "home-late/.rcl/runs"
        for label, owner, why in (("mo", "ffffffffffffffff", "label mo belongs to another driver (ffffffffffffffff)"),
                                  ("mn", None, "runs/mn exists, and was not made by run-ci-local")):
            (late / label / "jobs").mkdir(parents=True)
            if owner:
                (late / label / "owner").write_text(owner + "\n")
            answered = []

            def command(host, remote_command):
                if host.name == "late":
                    # not before the run has started: its check, and its setup after the label is loaded
                    if len(answered) < 2:
                        answered.append(True)
                        return ["false"]
                    return ["env", f"HOME={tmp}/home-late", "bash", "-c", remote_command]
                return ["env", f"HOME={tmp}/home-localhost", "bash", "-c", remote_command]
            (self.tmp / "flaky").touch()
            with self.subTest(label=label), patch.object(run_ci_local.Host, "command", command), \
                    patch("run_ci_local.HOST_RETRY_SECONDS", 0):
                out, code = self.run_stress(label, "--no-report")
            self.assertIn(f"WARNING: late: {why}; it is left out", out)
            self.assertIn(f"removed the stashes of {label} on localhost and locally", out)
            self.assertFalse((self.results / label / "stash").exists())
            self.assertEqual(3, code)
            self.assertTrue((late / label / "jobs").is_dir())

    def test_a_host_whose_runtime_does_not_answer_is_tried_again(self):
        self.hosts.write_text("down 8\nlocalhost 8\n")
        down_bin = self.tmp / "down-bin"
        write_executable(down_bin / "docker", '#!/bin/sh\n[ "$1" = info ] && exit 1\n'
                                              'echo "Docker version 27.0.0, build stub"\n')
        tmp, real_command = self.tmp, run_ci_local.Host.command

        def command(host, remote_command):
            if host.name == "down":
                return ["env", f"PATH={down_bin}:{os.environ['PATH']}", f"HOME={tmp}/home-down", "bash", "-c",
                        remote_command]
            return real_command(host, remote_command)
        (self.tmp / "flaky").touch()
        with patch.object(run_ci_local.Host, "command", command):
            out, code = self.run_stress("dn", "--no-report")
        self.assertIn("WARNING: down cannot be set up yet, and is tried again in 20s", out)
        self.assertIn("`docker info` fails", out)
        self.assertNotIn("left out", out)
        self.assertEqual(3, code)

    def test_a_host_that_does_not_answer_waits_longer_each_time(self):
        run = self.make_run()
        host, clock, installs = run.hosts[0], [1000.0], []

        def install(*_, **__):
            installs.append(clock[0])
            raise HostError("localhost: ssh: connect to host localhost port 22: Connection timed out")
        delays = []
        with patch.object(run_ci_local.Remote, "install", install), patch("run_ci_local.time.time", lambda: clock[0]), \
                contextlib.redirect_stdout(io.StringIO()):
            for _ in range(6):
                self.assertFalse(run.setup_host(host))
                retry_at, delay, _ = run.unready[host.name]
                clock[0] = retry_at - 1
                self.assertFalse(run.setup_host(host))
                delays.append(delay)
                clock[0] = retry_at
        self.assertEqual([20, 40, 80, 160, 300, 300], delays)
        self.assertEqual(6, len(installs))

    def test_no_host_names_the_hosts_that_do_not_answer(self):
        self.hosts.write_text("away 8\n")
        with patch.object(run_ci_local.Host, "command", lambda host, remote_command: ["false"]), \
                self.assertRaisesRegex(SystemExit, "no host can run cells: away: "):
            self.run_stress("na", "--no-report")

    def test_a_resumed_label_stops_its_cells_everywhere_first(self):
        (self.tmp / "flaky").touch()
        self.run_stress("rs", "--no-report", "--keep-stashes")
        # the stress cell was running on a host that has since left the hosts file
        gone_home = self.tmp / "gone-home"
        jobs = gone_home / "runs/rs/jobs"
        jobs.mkdir(parents=True)
        (gone_home / "runs/rs/owner").write_text(run_ci_local.owner_id(self.results.resolve()) + "\n")
        runner = fake_runner("rs", STRESS)
        self.addCleanup(runner.kill)
        (jobs / f"{STRESS}.pid").write_text(f"{runner.pid}\n")
        (jobs / f"{STRESS}.attempt").write_text("1-x\n")
        (jobs / f"{STRESS}.result.tar").write_text("partial")
        state_file = self.results / "rs/state.json"
        state = json.loads(state_file.read_text())
        state["cells"][STRESS] = {"name": "stress-test jdk11", "kind": "test", "status": "running", "attempt": 1,
                                  "attempt_id": "1-x", "host": "gone", "home": str(gone_home)}
        state_file.write_text(json.dumps(state))
        real_command = run_ci_local.Host.command

        def command(host, remote_command):
            return ["bash", "-c", remote_command] if host.name == "gone" else real_command(host, remote_command)
        with patch.object(run_ci_local.Host, "command", command):
            out, code = self.run_stress("rs", "--no-report")
        self.assertIsNotNone(runner.wait(timeout=10))
        self.assertIn("gone: stopped 1 running cells of rs, which run again", out)
        self.assertEqual([f"{STRESS}.attempt"], os.listdir(jobs))
        self.assertNotIn("STARTED jar", out)
        self.assertIn("STARTED stress-test jdk11 on localhost (attempt 1)", out)
        self.assertEqual(("done", 0, "localhost"), tuple(self.cell_state("rs", STRESS)[k]
                                                         for k in ("status", "rc", "host")))
        self.assertEqual(3, code)

    def test_a_resume_keeps_the_downloaded_results_only(self):
        self.run_stress("dk", "--no-report", "--keep-stashes")
        state_file = self.results / "dk/state.json"
        state = json.loads(state_file.read_text())
        # the second attempt was running, after a first that failed
        state["cells"][STRESS].update(status="running", attempt=2, attempt_id="2-x", done_attempts=1,
                                      last_done={"status": "done", "rc": 4, "attempt": 1, "host": "localhost",
                                                 "junit": 0})
        state_file.write_text(json.dumps(state))
        out, code = self.run_stress("dk", "--no-report")
        self.assertNotIn("STARTED jar", out)
        self.assertIn("STARTED stress-test jdk11 on localhost (attempt 2)", out)
        stress = self.cell_state("dk", STRESS)
        self.assertEqual(("done", 0, 2, 2), (stress["status"], stress["rc"], stress["attempt"],
                                              stress["done_attempts"]))
        self.assertEqual(3, code)

    def test_a_resumed_label_keeps_what_was_downloaded(self):
        out = self.results / "kd"
        for directory in ("a/attempt1", "a/attempt2", "b/attempt1", "c/attempt1"):
            (out / "cells" / directory).mkdir(parents=True)
        first = {"status": "done", "rc": 4, "attempt": 1, "host": "localhost"}
        (out / "state.json").write_text(json.dumps({"cells": {
            # interrupted on its second attempt, after a first that failed
            "a": {"name": "a", "status": "running", "attempt": 2, "attempt_id": "2-x", "host": "h", "done_attempts": 1,
                  "last_done": first},
            # lost, never downloaded
            "b": {"name": "b", "status": "lost", "attempt": 1, "attempt_id": "1-x", "host": "h"},
            "c": {"name": "c", "status": "done", "rc": 0, "attempt": 1, "done_attempts": 1}}}))
        run = self.make_run("kd")
        with patch.object(run_ci_local.Run, "stop_earlier_runs"):
            run.load_state([])
        cells = json.loads(run.state_file.read_text())["cells"]
        self.assertEqual(dict(first, name="a", done_attempts=1, last_done=first), cells["a"])
        self.assertEqual({"name": "b", "status": "not run", "rc": None}, cells["b"])
        self.assertEqual("done", cells["c"]["status"])
        self.assertEqual(["a/attempt1", "c/attempt1"], sorted(str(p.relative_to(out / "cells"))
                                                             for p in (out / "cells").glob("*/attempt*")))

    def test_a_cell_that_failed_its_downloaded_attempts_is_not_run_again(self):
        self.run_stress("fa", "--no-report", "--keep-stashes")
        state_file = self.results / "fa/state.json"
        state = json.loads(state_file.read_text())
        state["cells"][STRESS].update(rc=4, junit=0)
        state_file.write_text(json.dumps(state))
        out, code = self.run_stress("fa", "--no-report")
        self.assertNotIn("STARTED", out)
        self.assertEqual(4, code)

    def test_a_cell_lost_on_its_last_attempt_runs_again_on_resume(self):
        def lost(run, cell, host, attempt, attempt_id):
            return False if cell.kind == "test" else real_fetch(run, cell, host, attempt, attempt_id)
        real_fetch = run_ci_local.Run.fetch
        (self.tmp / "flaky").touch()
        with patch.object(run_ci_local.Run, "fetch", lost):
            _, code = self.run_stress("lo", "--no-report")
        stress = self.cell_state("lo", STRESS)
        self.assertEqual(("lost", 2), (stress["status"], stress["attempt"]))
        self.assertEqual(4, code)
        out, code = self.run_stress("lo", "--no-report")
        self.assertIn("STARTED stress-test jdk11 on localhost (attempt 1)", out)
        self.assertEqual(3, code)

    def test_a_resume_waits_for_a_host_that_does_not_answer(self):
        (self.tmp / "flaky").touch()
        self.run_stress("rw", "--no-report")
        self.hosts.write_text("away 8\nlocalhost 8\n")
        tmp, real_command, calls = self.tmp, run_ci_local.Host.command, []

        def command(host, remote_command):
            if host.name == "away":
                calls.append(remote_command)
                # the host check, then two asks to stop the label
                if len(calls) <= 3:
                    return ["false"]
                return ["env", f"HOME={tmp}/home-away", "bash", "-c", remote_command]
            return real_command(host, remote_command)
        with patch.object(run_ci_local.Host, "command", command), patch("run_ci_local.HOST_RETRY_SECONDS", 0):
            out, code = self.run_stress("rw", "--no-report", "--force")
        waited = out.index("WARNING: no cell of rw starts while one may still run on away; asking again in 0s")
        self.assertEqual(2, out.count("no cell of rw starts while one may still run on away"))
        self.assertLess(waited, out.index("STARTED jar jdk11"))
        self.assertEqual(3, code)

    def test_forget_host_gives_up_waiting(self):
        (self.tmp / "flaky").touch()
        self.run_stress("fh", "--no-report")
        self.hosts.write_text("away 8\nlocalhost 8\n")
        real_command = run_ci_local.Host.command

        def command(host, remote_command):
            return ["false"] if host.name == "away" else real_command(host, remote_command)
        with patch.object(run_ci_local.Host, "command", command):
            out, code = self.run_stress("fh", "--no-report", "--force", "--forget-host", "away")
        self.assertIn(f"WARNING: away is not asked to stop the cells of fh under {self.remote_home} "
                      f"(--forget-host)", out)
        self.assertNotIn("no cell of fh starts", out)
        self.assertEqual(3, code)

    def test_a_forgotten_host_stops_the_label_before_it_runs_a_cell(self):
        run = self.make_run()
        run.forgotten["localhost"] = [run.hosts[0]]
        events = []
        with patch.object(run_ci_local.Run, "stop_label_on", lambda r, h: events.append("stop")), \
                patch.object(run_ci_local.Run, "claim", lambda r, h: events.append("claim")), \
                contextlib.redirect_stdout(io.StringIO()):
            self.assertTrue(run.setup_host(run.hosts[0]))
        self.assertEqual(["stop", "claim"], events)
        self.assertEqual({"localhost": []}, run.forgotten)

    def test_a_new_label_stops_nothing(self):
        with patch.object(run_ci_local.Run, "stop_label_on", side_effect=AssertionError("stopped")):
            (self.tmp / "flaky").touch()
            _, code = self.run_stress("nl", "--no-report")
        self.assertEqual(3, code)

    def test_the_report_is_made_again(self):
        (self.tmp / "flaky").touch()
        self.run_stress("rr")
        out, code = self.run_stress("rr")
        self.assertIn("STARTED report", out)
        self.assertNotIn("STARTED stress-test", out)
        self.assertTrue((self.results / "rr/ci_summary.html").is_file())
        self.assertEqual(3, code)

    def test_the_hosts_a_label_ran_on_are_recorded_with_their_runtime(self):
        (self.tmp / "flaky").touch()
        self.run_stress("hr", "--no-report")
        state = json.loads((self.results / "hr/state.json").read_text())
        record = {"host": "localhost", "home": self.remote_home, "runtime": "docker",
                  "version": run_ci_local.remote_version()}
        self.assertEqual({f"localhost {self.remote_home}": record}, state["hosts"])
        self.assertEqual("docker", self.cell_state("hr", STRESS)["runtime"])

    def test_a_start_whose_answer_is_lost_is_stopped_before_another(self):
        self.hosts.write_text("broken 8\nlocalhost 8\n")
        self.remote_home = ".rcl"
        tmp, lost, stops = self.tmp, [], []
        real_try_start, real_stop = run_ci_local.Run.try_start, run_ci_local.Run.stop

        def command(host, remote_command):
            return ["env", f"HOME={tmp}/home-{host.name}", "bash", "-c", remote_command]

        def try_start(run, cell, host, attempt_id, reserve=False):
            started = real_try_start(run, cell, host, attempt_id, reserve)
            if host.name == "broken" and not lost:
                lost.append(attempt_id)
                raise HostError("broken: Connection reset by peer")
            return started

        def stop(run, cell, host, attempt_id):
            stops.append((host.name, attempt_id))
            return real_stop(run, cell, host, attempt_id)
        (self.tmp / "flaky").touch()
        with patch.object(run_ci_local.Host, "command", command), \
                patch.object(run_ci_local.Run, "try_start", try_start), patch.object(run_ci_local.Run, "stop", stop):
            out, code = self.run_stress("lc", "--no-report", "--keep-stashes")
        self.assertEqual(("broken", lost[0]), stops[0])
        self.assertLess(out.index("jar jdk11 cannot start on broken"), out.index("STARTED jar jdk11"))
        self.assertEqual(1, out.count("STARTED jar jdk11"))
        retired = tmp / "home-broken/.rcl/runs/lc/jobs/jar_jdk11_amd64.retired"
        self.assertIn(lost[0], retired.read_text().split())
        self.assertEqual(3, code)

    def test_a_host_that_fails_before_the_start_costs_no_attempt(self):
        self.hosts.write_text("broken 8\nlocalhost 8\n")
        real_upload, real_command, stops = run_ci_local.Run.upload, run_ci_local.Host.command, []

        def upload(run, host, path, name, version):
            if host.name == "broken":
                raise HostError("broken: Connection reset by peer")
            return real_upload(run, host, path, name, version)

        def command(host, remote_command):
            return real_command(run_ci_local.Host("localhost"), remote_command)
        (self.tmp / "flaky").touch()
        with patch.object(run_ci_local.Host, "command", command), patch.object(run_ci_local.Run, "upload", upload), \
                patch.object(run_ci_local.Run, "stop", lambda *a: stops.append(a)):
            out, code = self.run_stress("pf", "--no-report")
        self.assertIn("STARTED jar jdk11 on localhost (attempt 1)", out)
        self.assertEqual([], stops)
        self.assertEqual(3, code)

    def run_schedule(self, run, cells, wait, fetch=lambda *a: True, stop=lambda *a: "not running",
                     try_start=lambda *a, **k: True, inputs_for=lambda *a: []):
        """The scheduler with fake starts, waits and results"""
        run.parameters["fail_fast"] = True
        run.hosts[0].arch = "amd64"
        real_sleep = time.sleep
        with patch.object(run_ci_local.Run, "wait", wait), patch.object(run_ci_local.Run, "fetch", fetch), \
                patch.object(run_ci_local.Run, "stop", stop), patch.object(run_ci_local.Run, "try_start", try_start), \
                patch.object(run_ci_local.Run, "inputs_for", inputs_for), \
                patch.object(run_ci_local.Run, "host_free", lambda r: {h.name: (8, 8) for h in r.hosts}), \
                patch.object(run_ci_local.Host, "run", lambda *a, **k: subprocess.CompletedProcess(a, 0, b"", b"")), \
                patch("run_ci_local.time.sleep", lambda s: real_sleep(0.01)), contextlib.redirect_stdout(io.StringIO()):
            run.schedule(cells)
        return run.state["cells"]

    def test_fail_fast_keeps_the_result_of_a_cell_waiting_for_its_retry(self):
        run = self.make_run()
        lint, other = (Cell(step, {'type': 'build', 'splits': 1, 'timeout_hours': 1, 'script': 'x.sh'}, 1, '11', '3.8',
                            'no') for step in ('lint', 'other'))

        def try_start(r, cell, host, attempt_id, reserve=False):
            # no room for the retry of lint
            return not (cell is lint and attempt_id.startswith("2-"))
        cells = self.run_schedule(run, [lint, other], wait=lambda *a: 1, try_start=try_start)
        self.assertEqual("other jdk11", run.aborting)
        self.assertEqual(("done", 1, 1), tuple(cells[lint.cell_id][k] for k in ("status", "rc", "attempt")))

    def test_fail_fast_aborts_only_the_attempts_it_interrupted(self):
        run = self.make_run()
        first, earlier, ended, plain = (Cell(step, {'type': 'build', 'splits': 1, 'timeout_hours': 1,
                                                    'script': 'x.sh'}, 1, '11', '3.8', 'no')
                                        for step in ('first', 'earlier', 'ended', 'plain'))
        # earlier failed its first attempt in a run before
        run.state["cells"][earlier.cell_id] = {"name": earlier.name, "kind": "build", "status": "done", "rc": 1,
                                               "attempt": 1, "done_attempts": 1,
                                               "last_done": {"status": "done", "rc": 1, "attempt": 1}}
        (run.out / "cells" / earlier.cell_id / "attempt1").mkdir(parents=True)
        events = {c.cell_id: threading.Event() for c in (earlier, ended, plain)}

        def wait(r, cell, host, attempt_id, deadline):
            if cell is first:
                return 1
            events[cell.cell_id].wait(10)
            return 1 if cell is ended else None

        def stop(r, cell, host, attempt_id):
            if cell.cell_id in events:
                events[cell.cell_id].set()
            return "finished" if cell is ended else "stopped"
        cells = self.run_schedule(run, [first, earlier, ended, plain], wait=wait, stop=stop)
        self.assertEqual("first jdk11", run.aborting)
        self.assertEqual(("done", 1, 2), tuple(cells[first.cell_id][k] for k in ("status", "rc", "attempt")))
        # an interrupted attempt does not count: the earlier result stands
        self.assertEqual(("done", 1, 1), tuple(cells[earlier.cell_id][k] for k in ("status", "rc", "attempt")))
        # an attempt that had ended keeps its result
        self.assertEqual(("done", 1), tuple(cells[ended.cell_id][k] for k in ("status", "rc")))
        self.assertEqual("aborted", cells[plain.cell_id]["status"])

    def test_a_host_whose_capacity_cannot_be_read_waits_longer_each_time(self):
        run = self.make_run()
        host, installs = run.hosts[0], []
        run.ready.add(host.name)

        def unreachable(*_, **__):
            raise HostError("localhost: ssh: connect to host localhost port 22: Connection timed out")
        with patch.object(run_ci_local.Host, "run", unreachable), \
                patch.object(run_ci_local.Remote, "install", lambda *a, **k: installs.append(a)), \
                contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual({"localhost": (0, 0)}, run.host_free())
            self.assertEqual(20, run.unready["localhost"][1])
            self.assertEqual({"localhost": (0, 0)}, run.host_free())
        self.assertNotIn("localhost", run.ready)
        self.assertEqual([], installs)

    def test_clean_holds_the_lock_of_a_label_without_the_marker(self):
        (self.results / "nm/stash").mkdir(parents=True)
        lock = lock_label(self.results / "nm")
        self.addCleanup(lock.close)
        out, _ = self.run_ci_local("clean", "-l", "nm", "--stop")
        self.assertIn("clean: nm is running on this machine; kept", out)

    def test_clean_leaves_another_drivers_label_alone(self):
        (self.results / "cl/stash").mkdir(parents=True)
        (self.results / "cl" / run_ci_local.LABEL_MARKER).touch()

        def listing(owner, live):
            return lambda command: subprocess.CompletedProcess(command, 0, f"cl {live} 0 0 {owner}\n".encode(), b"")
        mine, theirs = FakeHost(listing(OWNER, 0), "mine"), FakeHost(listing(ONE, 1), "theirs")
        with contextlib.redirect_stdout(io.StringIO()) as out:
            run_ci_local.sweep([mine, theirs], OWNER, labels={"cl"}, results_dir=self.results)
        self.assertIn("theirs: cl belongs to another driver; left alone", out.getvalue())
        self.assertTrue(any(" rmlabel cl " in c for c in mine.commands))
        self.assertFalse(any(" rmlabel " in c for c in theirs.commands))
        self.assertFalse((self.results / "cl/stash").exists())

    def test_clean_removes_what_an_interrupted_claim_left(self):
        host = FakeHost(lambda command: subprocess.CompletedProcess(command, 0, b"", b""))
        with contextlib.redirect_stdout(io.StringIO()):
            run_ci_local.sweep([host], OWNER, labels={"cl"}, results_dir=self.results)
        self.assertTrue(any(" rmlabel cl " in c for c in host.commands), host.commands)


    def test_fail_fast_with_a_start_whose_answer_was_lost(self):
        run = self.make_run()
        first, unanswered = (Cell(step, {'type': 'build', 'splits': 1, 'timeout_hours': 1, 'script': 'x.sh'}, 1, '11',
                                  '3.8', 'no') for step in ('first', 'unanswered'))
        tried = []

        def try_start(r, cell, host, attempt_id, reserve=False):
            if cell is unanswered:
                tried.append(attempt_id)
                raise HostError("fake: Connection reset by peer")
            return True

        def inputs_for(r, cell):
            # the unanswered cell waits for its inputs once its start has gone out
            return None if cell is unanswered and tried else []
        result = {}

        def schedule():
            result["cells"] = self.run_schedule(run, [first, unanswered], wait=lambda *a: 1, stop=lambda *a: None,
                                                try_start=try_start, inputs_for=inputs_for)
        with patch("run_ci_local.HOSTS_UNREACHABLE_SECONDS", 0):
            thread = threading.Thread(target=schedule, daemon=True)
            thread.start()
            thread.join(30)
        self.assertFalse(thread.is_alive(), "the scheduler is stuck")
        self.assertEqual("first jdk11", run.aborting)
        self.assertEqual("not run", result["cells"][unanswered.cell_id]["status"])

    def test_a_lost_retry_keeps_the_downloaded_result(self):
        run = self.make_run()
        cell = Cell('earlier', {'type': 'build', 'splits': 1, 'timeout_hours': 1, 'script': 'x.sh'}, 1, '11', '3.8',
                    'no')
        run.state["cells"][cell.cell_id] = {"name": cell.name, "kind": "build", "status": "done", "rc": 1,
                                            "attempt": 1, "done_attempts": 1,
                                            "last_done": {"status": "done", "rc": 1, "attempt": 1}}
        (run.out / "cells" / cell.cell_id / "attempt1").mkdir(parents=True)
        cells = self.run_schedule(run, [cell], wait=lambda *a: None)
        self.assertEqual(("done", 1, 1), tuple(cells[cell.cell_id][k] for k in ("status", "rc", "attempt")))

    def test_a_write_of_an_earlier_run_is_refused(self):
        run = self.make_run("ew")
        host = run.hosts[0]
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertTrue(run.setup_host(host))
        stash = self.tmp / "stash.tar"
        stash.write_bytes(b"x" * 100)
        # a driver that resumed the label
        host.run(host.remote.job("kill", "ew", run.owner, "5e55105e55105e56"))
        with self.assertRaisesRegex(OwnerError, "taken over"):
            run.upload(host, stash, "src.tar", "v1")
        self.assertFalse((Path(self.remote_home) / "runs/ew/src.tar").exists())

    def test_a_resume_waits_for_a_stop_its_host_cannot_confirm(self):
        (self.results / "uc").mkdir(parents=True)
        (self.results / "uc/state.json").write_text(json.dumps({"cells": {}, "hosts": {
            "localhost h": {"host": "localhost", "home": self.remote_home}}}))
        run = self.make_run("uc")
        kills = []

        def answer(host, command, **_):
            if " kill " in command:
                kills.append(command)
                code = 1 if len(kills) <= 2 else 0
                return subprocess.CompletedProcess(command, code, b"0\n", b"a container of c could not be stopped")
            return subprocess.CompletedProcess(command, 0, b"", b"")
        with patch.object(run_ci_local.Host, "run", answer), patch.object(run_ci_local.Remote, "install"), \
                patch("run_ci_local.HOST_RETRY_SECONDS", 0), contextlib.redirect_stdout(io.StringIO()) as out:
            run.stop_earlier_runs(run.state)
        self.assertEqual(3, len(kills))
        self.assertEqual(2, out.getvalue().count("a container of c could not be stopped"))

    def test_a_refused_resume_stops_nothing(self):
        (self.tmp / "flaky").touch()
        self.run_stress("rf", "--no-report")
        (self.src / "new").write_text("x")
        self.commit("new")
        with patch.object(run_ci_local.Run, "stop_label_on", side_effect=AssertionError("stopped")), \
                self.assertRaisesRegex(SystemExit, "other inputs"):
            self.run_stress("rf", "--no-report")

    def test_forget_host_covers_every_remote_home_of_the_host(self):
        old_home = str(self.tmp / "old-home")
        (self.results / "fo").mkdir(parents=True)
        (self.results / "fo/state.json").write_text(json.dumps({"cells": {}, "hosts": {
            f"localhost {old_home}": {"host": "localhost", "home": old_home}}}))
        with self.assertRaisesRegex(SystemExit, "--forget-host elsewhere: not a host"):
            self.make_run("fo", "--forget-host", "elsewhere")
        run = self.make_run("fo", "--forget-host", "localhost")
        stops = []
        with patch.object(run_ci_local.Run, "stop_label_on", lambda r, h: stops.append(h.remote.given_home)), \
                patch.object(run_ci_local.Run, "claim", lambda r, h: stops.append("claim")), \
                contextlib.redirect_stdout(io.StringIO()):
            run.stop_earlier_runs(run.state)
            self.assertEqual([], stops)
            self.assertTrue(run.setup_host(run.hosts[0]))
        self.assertEqual(sorted([old_home, self.remote_home]), sorted(stops[:2]))
        self.assertEqual("claim", stops[2])

    def test_clean_locks_the_label_before_asking_a_host(self):
        held = []

        def install(*_, **__):
            lock = lock_label(self.results / "nd")
            held.append(lock is None)
            if lock is not None:
                lock.close()
        with patch.object(run_ci_local.Remote, "install", install):
            self.run_ci_local("clean", "-l", "nd")
        self.assertEqual([True], held)
        self.assertFalse((self.results / "nd").exists())

    def test_clean_keeps_the_stash_when_a_host_fails_to_remove_the_label(self):
        (self.results / "cl/stash").mkdir(parents=True)
        (self.results / "cl" / run_ci_local.LABEL_MARKER).touch()
        host = FakeHost(lambda command: subprocess.CompletedProcess(command, 1 if " rmlabel " in command else 0, b"",
                                                                     b"rm: cannot remove"))
        with contextlib.redirect_stdout(io.StringIO()):
            run_ci_local.sweep([host], OWNER, labels={"cl"}, results_dir=self.results)
        self.assertTrue((self.results / "cl/stash").is_dir())

    def test_no_report_entry_is_left_from_an_earlier_run(self):
        (self.tmp / "flaky").touch()
        self.run_stress("ne")
        self.run_stress("ne", "--no-report")
        self.assertNotIn("report", json.loads((self.results / "ne/state.json").read_text())["cells"])

    def test_capacity_backoff_ends_with_an_answered_probe(self):
        run = self.make_run()
        host, clock, probes = run.hosts[0], [1000.0], []
        run.ready.add(host.name)

        def answer(h, command, **_):
            if command.endswith(" capacity"):
                probes.append(clock[0])
                if len(probes) <= 2:
                    raise HostError("localhost: ssh: connect to host localhost port 22: Connection timed out")
                return subprocess.CompletedProcess(command, 0, b"8 8\n", b"")
            return subprocess.CompletedProcess(command, 0, b"amd64\n", b"")
        with patch.object(run_ci_local.Host, "run", answer), patch.object(run_ci_local.Remote, "install"), \
                patch.object(run_ci_local.Run, "claim"), patch("run_ci_local.time.time", lambda: clock[0]), \
                contextlib.redirect_stdout(io.StringIO()):
            run.host_free()
            self.assertEqual(20, run.unready["localhost"][1])
            clock[0] += 20
            # set up again, but the probe fails: the delay goes on doubling
            run.host_free()
            self.assertEqual(40, run.unready["localhost"][1])
            clock[0] += 40
            self.assertEqual({"localhost": (8, 8)}, run.host_free())
        self.assertNotIn("localhost", run.unready)

    def test_a_lost_attempt_is_retried_once_its_stop_is_confirmed(self):
        run = self.make_run()
        cell = Cell('lint', {'type': 'build', 'splits': 1, 'timeout_hours': 1, 'script': 'x.sh'}, 1, '11', '3.8', 'no')
        events = []

        def wait(r, c, host, attempt_id, deadline):
            return None if attempt_id.startswith("1-") else 0

        def stop(r, c, host, attempt_id):
            events.append("unconfirmed" if events.count("unconfirmed") < 2 else "stopped")
            return None if events[-1] == "unconfirmed" else "stopped"

        def try_start(r, c, host, attempt_id, reserve=False):
            events.append(f"start {attempt_id[0]}")
            return True
        with patch("run_ci_local.HOST_RETRY_SECONDS", 0):
            cells = self.run_schedule(run, [cell], wait=wait, stop=stop, try_start=try_start)
        self.assertEqual(["start 1", "unconfirmed", "unconfirmed", "stopped", "start 2"], events)
        self.assertEqual(("done", 0), (cells[cell.cell_id]["status"], cells[cell.cell_id]["rc"]))

    def test_clean_removes_nothing_of_a_label_whose_cells_it_could_not_stop(self):
        owned = Path(self.remote_home) / "runs/cs"
        (owned / "jobs").mkdir(parents=True)
        (owned / "owner").write_text(run_ci_local.owner_id(self.results.resolve()) + "\n")
        (owned / "jobs/c.runtime").write_text("docker\n")
        (owned / "jobs/c.containers").write_text("stuck\n")
        (self.results / "cs/stash").mkdir(parents=True)
        (self.results / "cs" / run_ci_local.LABEL_MARKER).touch()
        write_executable(self.runtime.directory / "docker", '#!/bin/sh\n[ "$1" = stop ] && exit 1\n'
                                                            '[ "$1" = inspect ] && echo true\nexit 0\n')
        with self.assertRaisesRegex(SystemExit, "the cells of cs could not all be stopped"):
            self.run_ci_local("clean", "-l", "cs", "--stop")
        self.assertTrue((owned / "jobs/c.containers").exists())
        self.assertTrue((self.results / "cs/stash").is_dir())

    def test_a_label_lock_is_never_removed_while_it_can_be_held(self):
        real_lock_label, runs = run_ci_local.lock_label, []

        class LetGo:
            """clean's lock, as another driver takes the label the moment clean lets go of it"""
            def __init__(self, lock):
                self.lock = lock

            def close(self):
                self.lock.close()
                runs.append(self_test.make_run("nd"))

        def lock_label(out):
            lock = real_lock_label(out)
            return LetGo(lock) if lock is not None and not runs and out.name == "nd" else lock
        self_test = self
        with patch.object(run_ci_local, "lock_label", lock_label):
            self.run_ci_local("clean", "-l", "nd")
        self.assertEqual(1, len(runs))
        with self.assertRaisesRegex(SystemExit, "another run-ci-local driver on this machine runs label nd"):
            self.make_run("nd")

    def test_a_resume_that_will_be_refused_stops_nothing(self):
        (self.tmp / "flaky").touch()
        self.run_stress("rv", "--no-report")
        cells = sorted(p.name for p in (self.results / "rv/cells").iterdir())
        not_dtests = self.tmp / "not-dtests"
        not_dtests.mkdir()
        subprocess.run(["git", "init", "-q", str(not_dtests)], check=True)
        subprocess.run(["git", "-C", str(not_dtests), "-c", "user.name=t", "-c", "user.email=t@example.com", "commit",
                        "-q", "--allow-empty", "-m", "x"], check=True)
        root_bin = self.tmp / "root-bin"
        write_executable(root_bin / "id", '#!/bin/sh\n[ "$1" = "-u" ] && echo 0\n')
        tmp = self.tmp

        def rooted(host, remote_command):
            return ["env", f"PATH={root_bin}:{os.environ['PATH']}", f"HOME={tmp}/home-rooted", "bash", "-c",
                    remote_command]
        for why, args, hosts in (
                ("not a cassandra-dtest checkout", ("--force", "-e", "dtest", "--dtest-dir", str(not_dtests)),
                 "localhost 8\n"),
                ("no host can run cells", ("--force",), "rooted 8\n")):
            self.hosts.write_text(hosts)
            with self.subTest(why=why), \
                    patch.object(run_ci_local.Run, "stop_label_on", side_effect=AssertionError("stopped")), \
                    patch.object(run_ci_local.Host, "command", rooted if "rooted" in hosts
                                 else run_ci_local.Host.command), \
                    self.assertRaisesRegex(SystemExit, why):
                self.run_ci_local("run", "-l", "rv", "--src", str(self.src), "-p", "custom", "-j", "11",
                                  "--no-report", *args)
            self.assertEqual(cells, sorted(p.name for p in (self.results / "rv/cells").iterdir()))

    def test_a_host_whose_label_directory_is_a_link_is_left_out(self):
        self.hosts.write_text("linked 8\nlocalhost 8\n")
        self.remote_home = ".rcl"
        tmp = self.tmp
        (tmp / "home-linked/.rcl/runs").mkdir(parents=True)
        (tmp / "elsewhere").mkdir()
        (tmp / "home-linked/.rcl/runs/sl").symlink_to(tmp / "elsewhere")

        def command(host, remote_command):
            return ["env", f"HOME={tmp}/home-{host.name}", "bash", "-c", remote_command]
        (self.tmp / "flaky").touch()
        with patch.object(run_ci_local.Host, "command", command):
            out, code = self.run_stress("sl", "--no-report")
        self.assertIn("WARNING: linked: runs/sl is a symbolic link, which run-ci-local does not use; it is left out",
                      out)
        self.assertNotIn("linked cannot be set up yet", out)
        self.assertEqual([], os.listdir(tmp / "elsewhere"))
        self.assertEqual(3, code)

    def test_a_label_whose_state_is_gone_is_taken_over(self):
        (self.tmp / "flaky").touch()
        self.run_stress("tg", "--no-report", "--keep-stashes")
        shutil.rmtree(self.results / "tg")
        shutil.rmtree(self.results / ".locks")
        out, code = self.run_stress("tg", "--no-report")
        self.assertIn("STARTED stress-test jdk11", out)
        self.assertEqual(3, code)
    def results_of(self, label):
        """The label's results and state, file by file, apart from its stash"""
        out = self.results / label
        return {str(p.relative_to(out)): p.read_bytes() for p in sorted(out.rglob("*"))
                if p.is_file() and p.relative_to(out).parts[0] != "stash"}

    def test_a_refused_rerun_leaves_the_results_as_they_were(self):
        (self.tmp / "flaky").touch()
        self.run_stress("rr9", "--no-report")
        before = self.results_of("rr9")
        not_dtests = self.tmp / "not-dtests"
        not_dtests.mkdir()
        (not_dtests / "README").write_text("x\n")
        for args in (["init", "-q", "-b", "trunk"], ["add", "."],
                     ["-c", "user.name=t", "-c", "user.email=t@example.com", "commit", "-q", "-m", "x"]):
            subprocess.run(["git", "-C", str(not_dtests), *args], check=True)
        with self.assertRaisesRegex(SystemExit, "not a cassandra-dtest checkout"):
            self.run_ci_local("run", "-l", "rr9", "--src", str(self.src), "-p", "custom", "-e", "dtest", "--cells",
                              "^dtest jdk11 1/64$", "-j", "11", "-d", f"file://{not_dtests}", "--no-report", "--force")
        self.assertEqual(before, self.results_of("rr9"))
        # a submodule that is not checked out
        head = subprocess.run(["git", "-C", str(self.src), "rev-parse", "HEAD"], stdout=subprocess.PIPE, text=True,
                              check=True).stdout.strip()
        subprocess.run(["git", "-C", str(self.src), "update-index", "--add", "--cacheinfo", f"160000,{head},module"],
                       check=True)
        subprocess.run(["git", "-C", str(self.src), "-c", "user.name=t", "-c", "user.email=t@example.com", "commit",
                        "-q", "-m", "module"], check=True)
        with self.assertRaisesRegex(SystemExit, "submodule module .* is not checked out"):
            self.run_stress("rr9", "--no-report", "--allow-sha-change")
        self.assertEqual(before, self.results_of("rr9"))
        self.assertFalse((self.results / "rr9/previous").exists())

    def test_a_force_rerun_killed_while_removing_results_is_finished_by_the_next(self):
        (self.tmp / "flaky").touch()
        self.run_stress("fk", "--no-report")
        cells, real_rmtree = self.results / "fk/cells", shutil.rmtree

        def killed_halfway(path, *args, **kwargs):
            if Path(path) == cells:
                real_rmtree(cells / STRESS)
                raise RuntimeError("killed")
            return real_rmtree(path, *args, **kwargs)
        with patch("run_ci_local.shutil.rmtree", killed_halfway), self.assertRaisesRegex(RuntimeError, "killed"):
            self.run_stress("fk", "--no-report", "--force")
        out, code = self.run_stress("fk", "--no-report")
        self.assertIn("STARTED jar jdk11", out)
        self.assertIn("STARTED stress-test jdk11", out)
        self.assertIn("tests: passed 1 | failed 1 | skipped 1", out)
        self.assertEqual(3, code)

    def test_a_change_of_inputs_killed_while_archiving_is_finished_by_the_next(self):
        (self.tmp / "flaky").touch()
        self.run_stress("ak", "--no-report")
        (self.src / "new").write_text("x")
        self.commit("new")
        real_replace, moved = os.replace, []

        def killed_halfway(source, target):
            if Path(target).parent.parent == self.results / "ak/previous" and Path(target).name == "summary.txt" \
                    and not moved:
                moved.append(target)
                raise RuntimeError("killed")
            return real_replace(source, target)
        with patch("run_ci_local.os.replace", killed_halfway), self.assertRaisesRegex(RuntimeError, "killed"):
            self.run_stress("ak", "--no-report", "--allow-sha-change")
        out, code = self.run_stress("ak", "--no-report")
        archives = list((self.results / "ak/previous").iterdir())
        self.assertEqual(1, len(archives))
        self.assertTrue(all((archives[0] / name).exists() for name in ("cells", "state.json", "summary.txt")))
        self.assertIn("STARTED stress-test jdk11", out)
        self.assertEqual(3, code)

    def test_idle_labels_are_removed_under_their_lock(self):
        (self.results / "il/stash").mkdir(parents=True)
        (self.results / "il" / run_ci_local.LABEL_MARKER).touch()
        taken = []

        def answer(command):
            if " rmlabel " in command:
                # another driver on this machine trying to run the label
                lock = lock_label(self.results / "il")
                taken.append(lock is not None)
                if lock is not None:
                    lock.close()
            return subprocess.CompletedProcess(command, 0, f"il 0 1 0 {OWNER}\n".encode(), b"")
        host = FakeHost(answer)
        with contextlib.redirect_stdout(io.StringIO()):
            run_ci_local.sweep([host], OWNER, idle_hours=1, results_dir=self.results)
        self.assertEqual([False], taken)
        self.assertFalse((self.results / "il/stash").exists())
        lock = lock_label(self.results / "il")
        self.assertIsNotNone(lock)
        lock.close()

    def test_new_inputs_take_the_dtest_branch_again(self):
        dtest = self.make_dtest_repository()
        first = self.dtest_head(dtest)
        (self.tmp / "flaky").touch()
        args = ("run", "-l", "dn", "--src", str(self.src), "-p", "custom", "-e", "dtest", "--cells",
                "^dtest jdk11 1/64$", "-j", "11", "-d", f"file://{dtest}", "-k", "trunk", "--no-report")
        self.run_ci_local(*args)
        (dtest / "new_test.py").write_text("\n")
        subprocess.run(["git", "-C", str(dtest), "add", "."], check=True)
        subprocess.run(["git", "-C", str(dtest), "-c", "user.name=t", "-c", "user.email=t@example.com",
                        "commit", "-q", "-m", "more"], check=True)
        # a resume of the same inputs keeps the dtests it ran
        self.run_ci_local(*args)
        self.assertEqual(first, json.loads((self.results / "dn/state.json").read_text())["dtest_commit"])
        (self.src / "new").write_text("x")
        self.commit("new")
        self.run_ci_local(*args, "--allow-sha-change")
        state = json.loads((self.results / "dn/state.json").read_text())
        self.assertEqual(self.dtest_head(dtest), state["dtest_commit"])
        self.assertNotEqual(first, state["dtest_commit"])

if __name__ == '__main__':
    unittest.main()
