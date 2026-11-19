# Help for `.build/run-ci-local`

- [What it runs](#what-it-runs)
- [Parameters and the config file](#parameters-and-the-config-file)
- [Hosts](#hosts)
- [Where a run keeps its data](#where-a-run-keeps-its-data)
- [Usage](#usage)
- [Results](#results)
- [Resuming a label](#resuming-a-label)
- [Cleaning up](#cleaning-up)
- [Known limitations](#known-limitations)
- [Podman](#podman)
- [Differences from Jenkins](#differences-from-jenkins)
- [Testing](#testing)

`.build/run-ci-local` runs the CI pipeline of `.jenkins/Jenkinsfile` on one or more hosts reached over ssh,
without Jenkins or Kubernetes: for the committed HEAD of a local checkout, or, as Jenkins does, for a branch of
a repository.

A host needs bash, GNU coreutils and findutils, `flock` and `setsid` (util-linux), `pkill`, `xz`, `bc`, and
docker or podman, and a user other than root: the `.build/docker` scripts create the container user with the
host user's uid.  The driver checks this on every host before it starts, and that `docker info` or
`podman info` works and that a container can write to a bind mount of the host's run directory, as the scripts'
containers do.  The scripts mount without SELinux relabelling, so a host enforcing SELinux fails this check.  A
host that lacks a tool, runs as root or fails the bind mount is left out of the run with its reason, as is one
where another driver, or something other than run-ci-local, has the label's directory.  A host that does not
answer, whose `info` fails or that cannot pull `alpine` is tried again while the run goes on, after 20 seconds,
then after twice as long each time, up to 5 minutes, as long as another host can start.  `status` and `clean`
check nothing and start no container.  The driver needs Python 3.8 or later, git and `xz`, and uses the Python
standard library only.

## What it runs

The cells are those the Jenkinsfile's `tasks()` creates for the same parameters.  The profiles, steps,
splits, architectures, agent sizes, timeouts and parameter defaults are read from the Jenkinsfile, and the
JDKs from `build.xml`, both of the commit that is tested.  A cell runs the same script with the same arguments
and variables as Jenkins:

| Jenkins | run-ci-local |
|---|---|
| `jar` stage, per architecture and JDK: `.build/docker/build-jars.sh <jdk>` in a shallow clone, then `stash` | `jar` cell, in `git archive` of the commit with its submodules and a depth-1 `.git`; the built tree becomes `base-<arch>-jdk<N>.tar`, copied to every host that needs it |
| build steps: `.build/docker/<script> <jdk>` | the same |
| test cells: `.build/docker/run-tests.sh -a <step> -c '<split>/<splits>' -j <jdk>` | the same, in a fresh copy of the jar stage's tree |
| `jvm-dtest-upgrade*`: `run-tests.sh -a build_dtest_jars` first, when no stash of the jars exists | always, in every cell |
| python dtests: a clone of `dtest_repository`/`dtest_branch` in `build/cassandra-dtest` | the same, or `--dtest-dir <checkout>` |
| agents labelled `cassandra-<arch>-small`/`medium`/`large` | hosts of that architecture; the cell takes the agent's cpus of the host's capacity |
| `timeout(...)` around a cell | the same, times `--timeout-factor` |
| a failed cell is retried once, on another agent | the same, on another host when one has room |
| `failFast`, see below | the same, unless `--no-fail-fast` |
| `generateTestReports()`: `ci_summary.html`, `results_details.tar.xz` | the same scripts, run on one host after the cells |
| `processJmhReports()`: `test/output/combined-result.json` | the same file |

When the Jenkinsfile changes in code the tool reproduces, `run` warns, and `run-ci-local-test.py` fails
until both agree again.

### Fail fast

The `Tests` stage of the Jenkinsfile starts after the whole `jar` stage.  Both stages run their cells in
parallel with `failFast: true`.  A cell fails when its script exits non-zero,
it times out, or, for a test cell, it leaves no JUnit file, on both of its attempts.  Then:

- in the `jar` stage, the other jar cells are interrupted, and the stage fails, so no other cell runs;
- in the `Tests` stage, every other running cell is interrupted, and none starts.  `test()` does not retry an
  interrupted cell.  (`build()` treats the interruption as a retryable error, so an interrupted build cell may
  run its second attempt; run-ci-local stops it.)
- the failed stage makes Jenkins skip the `Summary` stage: there is no `ci_summary.html`.

`run-ci-local` does the same by default: the other cells wait until every jar cell has finished, and when a
cell fails its last attempt, it stops the running cells, starts no more, and makes no `ci_summary.html`;
`summary.txt` lists what ran.  With `--no-fail-fast` a cell starts once its own jar cell has built, every cell
runs, except those of an architecture and JDK whose jar cell failed, and the report is made.

## Parameters and the config file

Every parameter of the Jenkins job is a command line option and a key of the config file.  The command line
wins over the config file, and the config file over the defaults.  A resumed label keeps the parameters it
first ran with; with `--force` the options given may differ, and those not given keep their earlier values.
`--fail-fast` and `--no-fail-fast` change without it.

`--profile-custom-regexp` and `--cells` are Python regexes (the `re` module), where Java's named groups
`(?<name>...)`, which the Jenkinsfile's `==~` takes, also work.  `run` prints the parameters it uses, as the
Jenkinsfile's `init` stage does, and keeps them in `state.json`.

| Jenkins parameter | option | config key | default |
|---|---|---|---|
| `repository` | `-r`, `--repository` | `repository` | the checkout is tested; with only `--branch`, its upstream repository |
| `branch` | `-b`, `--branch` | `branch` | the checkout is tested; with only `--repository`, the checkout's branch |
| `profile` | `-p`, `--profile` | `profile` | the Jenkinsfile's: `skinny` |
| `profile_custom_regexp` | `-e`, `--profile-custom-regexp` | `profile_custom_regexp` | empty; a Python regex, see below |
| `architecture` | `--architecture` | `architecture` | the Jenkinsfile's first: `amd64` |
| `jdk` | `-j`, `--jdk` | `jdk` | empty: every JDK of `build.xml` |
| `dtest_repository` | `-d`, `--dtest-repository` | `dtest_repository` | the Jenkinsfile's: `https://github.com/apache/cassandra-dtest` |
| `dtest_branch` | `-k`, `--dtest-branch` | `dtest_branch` | the Jenkinsfile's: `trunk` |
| (`failFast: true`) | `--fail-fast`, `--no-fail-fast` | `fail_fast` | yes |
| | `--hosts` | `hosts` | `$RUN_CI_LOCAL_HOSTS`, else `~/.config/cassandra/run-ci-local-hosts` |
| | `--remote-home` | `remote_home` | `.run-ci-local` |
| | `--results-dir` | `results_dir` | `build/ci-local` |
| | `--maven-mirror` | `maven_mirror` | none: Maven Central |
| | `--timeout-factor` | `timeout_factor` | 1 |
| | `--report`, `--no-report` | `report` | yes |
| | `--dtest-dir` | `dtest_dir` | none: fetch `dtest_repository` |

The config file is `--config <file>`, else `$RUN_CI_LOCAL_CONFIG`, else
`~/.config/cassandra/run-ci-local.conf`, in INI form with one `[run-ci-local]` section; see
[run-ci-local.conf.example](run-ci-local.conf.example).

With `--repository` or `--branch`, the branch is fetched into the label's stash with its submodules, and that
commit is tested and reported, as Jenkins does.  Without them, the checkout's committed HEAD is tested, and
the report names the checkout's upstream repository and branch, or its commit when it has no upstream.  The
python dtests run at the commit `dtest_branch` had when the label first needed them; a resumed label keeps it,
and `--force`, or `--allow-sha-change` with new commits or another `build.properties`, fetches the branch
again, as a new Jenkins build does.

## Hosts

A hosts file lists the hosts, see [hosts.example](hosts.example).  The driver connects with `ssh` in batch
mode, so keys must work without prompts, and runs every command with `bash -c`, whatever the login shell.
`localhost` runs cells on this machine without ssh.  A host's architecture is detected, or given as
`arch=amd64` or `arch=arm64`; cells run only on hosts of their architecture, and `run` stops when none has it.

Every host keeps its state in `~/.run-ci-local/` (`--remote-home`, or `home=` on the host's line; a path
without whitespace, absolute or relative to the remote home directory):

- `bin/<version>/`: `job.sh` and the podman wrapper, installed by the driver.
- `capacity`: the host's capacity, in cpus, shared by the drivers that use the same remote home as the same
  user on the host.  Cells take the size of their Jenkins agent: 1, 3 or 7.  The jar and build cells but
  `lint` take 1, as their agents are small, and `lint` takes 3, but their containers have no cpu limit and may
  use every core.  A host's capacity is set when it has none: from the hosts file, else to its core count.
  `run-ci-local set-capacity` replaces it with the hosts file's.
- `slots/`: one file per unit of capacity.  A running cell holds a `flock` on as many as it takes, so any
  number of drivers and labels share a host, and a cell that dies frees its slots.  Every held slot counts, so
  lowering the capacity never lets more cells start than it allows.  A cell that waited 5 minutes reserves its
  units in `reservations/`, so that smaller cells cannot starve it.
- `runs/<label>/`: the label's stashes (`src.tar`, `base-<arch>-jdk<N>.tar`, `dtest.tar`, about 1 GB a JDK),
  its running cells, and its `owner`: the machine and results directory of the driver that runs it.  It is
  made whole aside and renamed into place, so a claim that is interrupted leaves no directory without its
  owner.  Another driver does not run a label it does not own, even with `--force`: use another label.
  `clean --label <label> --force` removes another driver's label when none of its cells runs.
- `locks/`: the per-label and image build locks.

## Where a run keeps its data

- On this machine, `<results-dir>/<label>/` (`build/ci-local/<label>/` by default): the results, `state.json`,
  the marker `.run-ci-local`, and while the label runs `stash/`, with the source and build tarballs and the
  checkout fetched for `--repository`/`--branch`.  `clean` and the end of a run remove `stash/`, only from a
  directory with the marker.  The label's lock is `<results-dir>/.locks/<label>.lock`, which nothing removes.
- On each host, the remote home (`~/.run-ci-local/`, `--remote-home`, or the host's `home=`): `bin/`,
  `capacity`, `slots/`, `reservations/`, `locks/`, and `runs/<label>/` while the label runs.  A label's
  directory is made by the run that owns it, and only directories with its owner marker are removed.
- Outside the remote home, the container runtime keeps the images the cells build or pull (`apache/cassandra-*`,
  `alpine`) and their build cache: under `~/.local/share/containers/` for rootless podman, or docker's data root.
  The tool does not remove them, so that the next run finds them; `podman rmi` or `docker rmi` does.

## Usage

```bash
# the parameters and cells of the skinny profile, without running them
.build/run-ci-local run --label my-change --dry-run

# the pre-commit profile on JDK 11
.build/run-ci-local run --label my-change --profile pre-commit --jdk 11

# a branch of a fork, as Jenkins would test it
.build/run-ci-local run --label my-branch --repository https://github.com/example-user/cassandra \
    --branch my-branch

# some steps, or some cells: a regexp of cell names, which are Jenkins' stage names
.build/run-ci-local run --label my-change --profile custom --profile-custom-regexp 'stress-test|jvm-dtest'
.build/run-ci-local run --label my-change --profile pre-commit --jdk 11 --cells '^test jdk11 (3|11)/20$'

# some tests in every test step of the profile, with run-tests.sh -t in place of splits
.build/run-ci-local run --label rerun --profile custom --profile-custom-regexp test \
    --test 'CompactionsTest|ScrubTest'

# the build steps on arm64 hosts
.build/run-ci-local run --label my-change --profile packaging --architecture arm64

# free capacity, architecture and runtime of each host, and the running cells as their hosts see them
.build/run-ci-local status --label my-change
.build/run-ci-local summary --label my-change  # the summary again
.build/run-ci-local clean --label my-change    # remove the label's stashes, see Cleaning up
```

Running a label again resumes it, see [Resuming a label](#resuming-a-label).  Only one driver on a machine
runs a label at a time.  A label belongs to one commit and one `build.properties`; use a new label after a
change, or `--allow-sha-change`, which moves the earlier results to `previous/` and runs every cell again.

The exit code is 0 for `SUCCESS`, 3 for `UNSTABLE` (failing tests) and 4 for `FAILURE`: a cell that failed,
was lost, was stopped, did not run, could start on no host, or, for a test step, left no test results.  It is 1
when the run cannot start or go on (a host fails its checks, another driver owns the label, the checkout cannot
be read), and 2 for a usage error.  The result is that of the cells the last run planned.

The checkout's untracked `build.properties`, when it has one, goes with the sources, as does what
`--maven-mirror` sets.  Only committed changes are tested.

Run the driver detached to have it survive the terminal:
`setsid nohup .build/run-ci-local run --label my-change > build/ci-local/my-change.log 2>&1 &`

## Results

`build/ci-local/<label>/` (`--results-dir`) holds:

- `ci_summary.html` and `results_details.tar.xz`: the files to attach to a JIRA ticket, as the Jenkins
  pipeline publishes them.  The summary names the commit tested, and lists the build parameters with the
  build tag `run-ci-local-<label>`.
- `generateTestReports.log.xz`: the output of the scripts that made them.  As in Jenkins, a test target whose
  JUnit report fails does not stop the others, nor `ci_summary.html`.
- `test/output/combined-result.json`: the JMH results of the cells, when they ran benchmarks.
- `summary.txt`: every cell with its host, attempts, exit code and test counts, the failing tests, and the
  result, as for the exit code.
- `cells/<cell>/attempt<N>/`: `stage.log.xz`, the full output of the cell's script; `rc`; `output/`, the
  JUnit XML (`.xml.xz`) laid out as Jenkins archives it; `logs/`, the container logs and the test logs; and the
  cell's timing record for `.build/ci/cell_balance.py`.
- `state.json`: the parameters, the dtest commit, the planned cells, the hosts the label ran on with their
  remote home, runtime and `job.sh` version, and every cell with its attempts.  A start that went out without
  an answer is stopped on its host before the cell starts anywhere else, and `job.sh` never starts an attempt
  that was stopped or replaced.  An attempt fail fast interrupted does not count towards the cell's two, and
  the cell keeps an earlier result it had; one that had ended, or failed on its own, keeps its result.  The
  summary, the report and the JMH results take each cell's recorded attempt.
- `previous/<n>-<version>/`: the results of the inputs the label ran before `--allow-sha-change`.

## Resuming a label

A driver that starts on a label that ran before, with or without `--force` or `--allow-sha-change`, first checks
everything that could refuse the run: the commit, `build.properties`, the parameters, the source and dtest
archives, which it makes then, and that a host can run the cells.  Only then does it stop every cell of the
label on the hosts of the hosts file and on the hosts `state.json` records, and wait until each host says none
runs, its runner and its containers; it never follows a cell an earlier driver started.  Hosts of the hosts file
that `state.json` does not record are asked too, as a driver that was killed may have started a cell there
before it recorded it.  From then on, only this driver's claims, starts and writes are taken by the hosts: one
of the earlier driver that arrives late is refused.  A label whose `state.json` is gone is taken over the same
way on a host that still has it.  The earlier results that `--allow-sha-change` moves to `previous/` or
`--force` removes are recorded in `state.json` first, and a driver killed meanwhile leaves the next one to
finish the move or the removal.  Then:

- a cell whose result was downloaded and recorded as done keeps it, and is not run again when it succeeded or
  failed its second attempt;
- every other cell runs again: one that was running, starting or lost when the earlier driver stopped, or that
  did not run.  Its unfinished attempt is discarded on the host and here, and does not count towards the
  cell's two;
- the report is always made again;
- the parameters are the label's, and `--force` runs every cell again.

A host that does not answer, or cannot confirm that the cells and their containers have stopped, is asked again
after 20 seconds, then twice as long each time, up to 5 minutes, and no cell of the label starts while it may
still run one there.  `--forget-host <host>` gives up waiting for a host, under every remote home the label used
there: should it answer later in the run, it stops the label's cells under each of them before it runs one.  A
name that is neither in the hosts file nor in `state.json` is an error.  The same goes during a run: a cell whose
attempt was lost, or whose start went out without an answer, is not tried again anywhere until its host
confirms that the attempt has stopped, or the host is given to `--forget-host`.

A host whose `runs/<label>` is a symbolic link, or leads out of `runs/`, is left out with that reason.

## Cleaning up

When every cell has finished, the driver removes the label's `runs/<label>/` on every host it used, and its
local `stash/`, which it keeps when a host could not remove the label.  The results stay.  `--keep-stashes`
keeps them for a `--force` rerun; remove them with `clean --label <label>` afterwards.  A failure to clean
up after a cell's result is saved is logged with what `job.sh` said, and changes nothing about the cell.

A driver that is stopped cleans nothing.  `clean --label <label>` removes the label's stashes, on the hosts of
the hosts file and on those its attempts ran on, and with `--stop` first stops its running cells; when a host
cannot confirm that they have stopped, nothing of the label is removed, on any host or here, and `clean` exits
non-zero.  It asks every host first: while one of them does not answer, or still runs a cell of the label, it
removes nothing, on any host, and says why.  It takes the label's lock on this machine before it asks any host,
and holds it throughout, as a run does; it keeps the local stash when a host fails to remove the label, and
removes it only from a directory with the marker.  A host where the label is another driver's, or not
run-ci-local's, holds nothing of the label and keeps no stash.  It also removes what an interrupted claim left
(`runs/.claim-<label>`).  Each `run` also removes the stashes of labels idle for 24 hours, holding each label's
lock while it does, and `clean --idle-hours <hours>` does the same by hand.  A label whose driver runs on this
machine or whose cell runs on a host is never removed.

## Known limitations

- If `clean --stop` cannot confirm a stop on one host, hosts that did confirm may already have deleted cell results
  and job metadata.
- If a host is unreachable during setup, `clean --stop` can exit 0 while the stashes remain.
- If the container runtime fails to stop a container when its runner exits, result cleanup can remove its tracking
  metadata, and a later `kill` can succeed while the container still runs.
- While the container runtime can't list or inspect containers, `stop`, `kill`, `start` and `clean --stop` fail and
  are retried rather than guessing; repeated exit 77 from `job.sh start` can leave a cell pending indefinitely.
- When `job.sh start` exits 77, the driver may start the cell on another host while the earlier attempt's containers
  still run on the first.
- A delayed `claim` can recreate a label after cleanup removed its session record.
- A `kill` from an earlier driver that was killed while taking over the label can arrive late and reset the label's
  session on a host; the resumed driver then stops with "taken over by another session" and must be started again.
- A cell whose attempt was lost on a host that is gone for good waits up to an hour for that host to confirm the
  stop, then fails as unstartable; restart with `--forget-host <host>` to run it elsewhere sooner.
- A host that accepts ssh but hangs on `job.sh stop` delays scheduling by up to 10 minutes each time a lost
  attempt's stop is asked.
- If another driver claims a label on a host between this driver's owner check and its claim, a resume stops after
  it has already stopped the label's earlier cells.
- A failure after the earlier cells were stopped (for example every host failing its claim) ends a resume with those
  cells stopped and, with `--force`, the earlier results removed.

## Podman

The runtime is detected on each host, or given in the hosts file.  On docker the `.build/docker` scripts
run unchanged.  On podman, `remote/podman/docker` stands in for `docker`:

- rootless podman: `run` gets `--userns=keep-id --passwd=false`, so the container user that
  `_create_user.sh` makes with the host's uid is the host user, and the build directory it writes stays
  the host user's.  A Dockerfile uid above the subordinate uid range is mapped into it for `build`.
- `build` drops `--load`, which podman does not have, and builds each image once per host.
- `info` adds the ` CPUs: <n>` line of `docker info`.
- `run` gets `--cpus=<cell size>` in place of the host's core count.

The last three are for checkouts whose `run-tests.sh` and `_docker_run.sh` predate their podman support.
Files a container leaves that the host user cannot remove are removed from a container.

## Differences from Jenkins

- Capacity counts cpus only.  Jenkins agents also have 1, 5 or 16 GB of memory, and the python dtests run
  with `-m 15g`; set a host's capacity low enough for its memory.
- `benchmark: true` steps (`microbench`) need dedicated agents in Jenkins; here they share hosts like any cell.
- With `--no-fail-fast`, the cells of the other architectures and JDKs run after a jar cell fails; in Jenkins
  a failed jar stage stops the pipeline.
- A host that has no image a script needs pulls it from the default registry, and builds it when that fails
  (`run-tests.sh` and `_docker_run.sh` do that); Jenkins first fetches them from the `apache.jfrog.io`
  registry.
- Nothing is sent to nightlies.apache.org, Slack or email.

## Testing

```bash
python3 .build/run-ci-local.d/run-ci-local-test.py
```

The tests need no container runtime: stub `docker` and `podman` commands stand in.  What they cannot show
needs a run on a podman host: that no process the cell started, such as podman's `fuse-overlayfs` and
`conmon` helpers, still holds a `slots/slot.*` file once the cell has ended.  With `groovy` on the
`PATH`, or `GROOVY_JAR` naming a Groovy 4 jar, they also run the Jenkinsfile's `tasks()` through
`jenkinsfile-cells.groovy` and compare its cells with this tool's, for every profile, JDK and architecture.
