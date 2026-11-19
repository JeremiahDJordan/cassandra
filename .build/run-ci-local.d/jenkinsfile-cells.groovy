// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
//
// Prints the names of the cells tasks() of .jenkins/Jenkinsfile creates, one per line, for run-ci-local-test.py
// to compare with .build/run-ci-local.  The pipeline block, the Jenkins steps tasks() calls, and the Jenkins classes
// the file names are stubbed.
//
// Usage: groovy jenkinsfile-cells.groovy <cassandra dir> <profile> <jdk> <architecture> <profile_custom_regexp>
//

def cassandraDir = new File(args[0])
def source = new File(cassandraDir, '.jenkins/Jenkinsfile').text
    .replaceFirst(/^#!.*\n/, '')
    .replaceAll(/@NonCPS/, '')
    .replaceAll(/hudson\.AbortException/, 'RuntimeException') + '''
def pipeline(Closure body) { }
def readFile(Map args) { new File(cassandraDir, args.file).text }
def echo(message) { }
'''
def parameters = [profile: args[1], jdk: args[2], architecture: args[3], profile_custom_regexp: args[4]]
def script = new GroovyShell(new Binding([params: parameters, cassandraDir: cassandraDir])).parse(source)
script.run()
def tasks = script.tasks()
(tasks.jars.keySet() + tasks.tests.keySet()).findAll { it != 'failFast' }.each { println it }
