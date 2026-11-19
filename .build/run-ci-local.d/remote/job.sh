#!/bin/bash
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
# The per-host side of .build/run-ci-local.  The driver installs it as <home>/bin/<version>/job.sh.
#
#   job.sh init [<capacity>]          writes <home>/capacity when it does not exist: the given units, else nproc
#   job.sh set-capacity <capacity>    replaces <home>/capacity
#   job.sh preflight                  prints what keeps this host from running cells, if anything; exits 1 when a
#                                     check fails that a retry cannot mend, else 3 when one fails that may pass later
#   job.sh capacity                   prints "<free units> <capacity>"
#   job.sh runtime                    prints the container runtime the cells use: "docker" or "podman [rootless]"
#   job.sh arch                       prints the host's architecture, as the Jenkinsfile names them: amd64, arm64
#   job.sh owner <label> <owner>      exits 0 when the owner may claim runs/<label>, 76 when another owner has it or
#                                     this tool did not make it, and 79 when it is not a directory in runs/
#   job.sh claim <label> <owner> <session>
#                                     creates runs/<label> for the owner and the session, or accepts the owner's own of
#                                     the same session; exits 76 when another owner has it, or it is a directory this
#                                     tool did not create, 78 when another session has it, and 79 as owner does
#   job.sh start <label> <cell> <units> <owner> <attempt> <session> [reserve]
#                                     takes <units> slots without waiting and starts the cell detached; the cell's
#                                     parameters are in runs/<label>/jobs/<cell>.env.  Exits 75 when the host is full,
#                                     77 while an earlier attempt of the cell or its containers still run, 78 for an
#                                     attempt that was stopped or replaced, or of another session, and 0 without
#                                     starting anything when this attempt has started already
#   job.sh unreserve <label> <cell>   drops the cell's reservation
#   job.sh status <label> <cell> <attempt>
#                                     prints "done <rc>", "running", "lost", or "unknown" for an attempt that is not
#                                     the cell's latest: one that never started, or that a later one replaced
#   job.sh put <label> <session> [jobs/]<name> <bytes> [<version>]
#                                     writes stdin to runs/<label>/[jobs/]<name>, and the version to <name>.version,
#                                     when it has the bytes given and the session is the label's; exits 78 for
#                                     another session.  The version is removed before the file is replaced
#   job.sh mark <label> <session> <name> <version>
#                                     writes the version of runs/<label>/<name>, when the session is the label's
#   job.sh stop <label> <cell> <attempt>
#                                     stops the attempt of the cell and its containers, if it is the latest, and
#                                     keeps it from starting later; prints "stopped" when it interrupted the attempt,
#                                     "finished" when it had ended, else "not running".  Exits 1 unless the runner
#                                     has gone and the runtime says each container of the cell has exited or is gone
#   job.sh clean <label> <cell> <attempt> <session>
#                                     removes the attempt's result and state files, if it is the latest and the
#                                     session is the label's
#   job.sh labels                     prints "<label> <running cells> <newest mtime> <stash bytes> <owner>" per label
#   job.sh kill <label> <owner> [<session>]
#                                     stops the label's cells and their containers, waits until none runs, and
#                                     discards their results; prints how many ran.  With a session, only that
#                                     session starts cells or writes stashes after.  Exits 76, touching nothing, for
#                                     a directory this tool did not make or another owner's, 79 for one that is not a
#                                     directory in runs/, and 1 when a runner or a container could not be stopped
#   job.sh rmlabel <label> <owner> [force]
#                                     removes runs/<label>, and what an interrupted claim left; exits 76 when another
#                                     owner has it, without force, or it is a directory this tool did not create, and
#                                     1 while one of its cells runs
#
# An attempt id names one start of a cell.  Only the latest attempt's runner writes the cell's result, and only it
# is stopped or cleaned, so that a runner left over from an earlier attempt cannot touch a new one.  An attempt that
# was stopped or replaced never starts, nor one of a session other than the label's, so that a start that arrives
# late cannot run beside the cells of a driver that has taken over.
#
# Capacity is in units of the Jenkins agent cpus (small=1, medium=3, large=7).  A slot is a flock(1)ed file
# slots/slot.N, held for a running cell by a holder process that ends with the cell, so any number of drivers
# can share a host and a cell that dies frees its slots.  Every held slot counts against the capacity, also one
# above a capacity that was lowered while it was held.
#
set -u
bin_dir="$(cd "$(dirname "$0")" && pwd -P)"
home="$(cd "${bin_dir}/../.." && pwd -P)"
slots="${home}/slots"
mkdir -p "${slots}" "${home}/runs"

usage() {
  echo "usage: $0 init|set-capacity|preflight|capacity|runtime|arch|owner|claim|put|mark|start|unreserve|status" \
       "|stop|clean|labels|kill|rmlabel …" >&2
  exit 2
}

# labels, cells and owners are single path components
check_names() {
  local name
  for name in "$@"; do
    [[ "${name}" =~ ^[A-Za-z0-9_][A-Za-z0-9_.-]*$ ]] || { echo "invalid name '${name}'" >&2; exit 2; }
  done
}

capacity() {
  cat "${home}/capacity" 2>/dev/null || nproc
}

free_slots() {
  local held=0 file fd free
  for file in "${slots}"/slot.*; do
    [ -f "${file}" ] || continue
    exec {fd}>>"${file}"
    flock -n "${fd}" || held=$((held + 1))
    exec {fd}>&-
  done
  free=$(( $(capacity) - held ))
  echo $(( free < 0 ? 0 : free ))
}

# sets runtime (docker, podman or none) and rootless (yes or no).  $CI_LOCAL_RUNTIME overrides the detection.
# podman's docker emulation answers `docker --version` with "podman version …"
detect_runtime() {
  runtime="${CI_LOCAL_RUNTIME:-}"
  if [ -z "${runtime}" ]; then
    if command -v docker >/dev/null 2>&1 && ! docker --version 2>/dev/null | grep -qi podman; then
      runtime=docker
    elif command -v podman >/dev/null 2>&1; then
      runtime=podman
    else
      runtime=none
    fi
  fi
  rootless=no
  if [ "${runtime}" == "podman" ] \
     && [ "$(podman info --format '{{.Host.Security.Rootless}}' 2>/dev/null)" == "true" ]; then
    rootless=yes
  fi
}

# A cell's build tree holds files the container created, which the host user may not own: root's under
# docker, or a subordinate uid's under rootless podman.  Root in a container can remove them in both.
remove_tree() {
  [ -e "$1" ] || return 0
  rm -rf "$1" 2>/dev/null && return 0
  detect_runtime
  [ "${runtime}" != "none" ] || return 1
  "${runtime}" run --rm -v "$(dirname "$1")":/parent alpine:3.19.1 rm -rf "/parent/$(basename "$1")" >/dev/null
  [ ! -e "$1" ]
}

# true while the runner of the cell runs: the pid is alive and is that runner, not a reused pid
runner_alive() {
  local pid
  pid="$(cat "${home}/runs/$1/jobs/$2.pid" 2>/dev/null)" || return 1
  [[ "${pid}" =~ ^[0-9]+$ ]] && kill -0 "${pid}" 2>/dev/null || return 1
  tr '\0' ' ' < "/proc/${pid}/cmdline" 2>/dev/null | grep -qF "job.sh _run $1 $2 "
}

# stops the containers of a cell: those the podman wrapper recorded, and those bind mounting its tree; fails unless
# the runtime says each of them has exited or is gone
stop_cell_containers() {
  local jd="${home}/runs/$1/jobs/$2" id source ids mounts failed=0
  local runtime
  runtime="$(cat "${jd}.runtime" 2>/dev/null)"
  [ -n "${runtime}" ] && [ "${runtime}" != "none" ] || return 0
  ids="$("${runtime}" ps -q 2>/dev/null)" || { echo "\`${runtime} ps\` fails" >&2; return 1; }
  if [ -f "${jd}.containers" ]; then
    while IFS= read -r id; do
      [ -z "${id}" ] || stop_container "${runtime}" "${id}" || failed=1
    done < "${jd}.containers"
  fi
  for id in ${ids}; do
    if ! mounts="$("${runtime}" inspect --format '{{range .Mounts}}{{.Source}}{{"\n"}}{{end}}' "${id}" \
                   2>/dev/null)"; then
      # one that ended since ps is gone; of one still there it is unknown whose it is
      container_gone "${runtime}" "${id}" || failed=1
      continue
    fi
    while IFS= read -r source; do
      if [ "${source}" == "${jd}" ] || [[ "${source}" == "${jd}/"* ]]; then
        stop_container "${runtime}" "${id}" || failed=1
        break
      fi
    done <<< "${mounts}"
  done
  [ "${failed}" == "0" ] || echo "a container of $2 could not be stopped, or the runtime could not tell" >&2
  return "${failed}"
}

# true when the runtime lists every container, and not this one
container_gone() {
  local all
  all="$("$1" ps -a -q --no-trunc 2>/dev/null)" || return 1
  ! grep -q "^$2" <<< "${all}"
}

# stops a container; true when the runtime says it has exited, or is gone
stop_container() {
  local running
  "$1" stop -t 5 "$2" >/dev/null 2>&1
  if running="$("$1" inspect --format '{{.State.Running}}' "$2" 2>/dev/null)"; then
    [ "${running}" == "false" ]
  else
    container_gone "$1" "$2"
  fi
}

# stops the runner of a cell and its containers; fails when the runner or a container is still there
stop_cell() {
  local pid i
  if runner_alive "$1" "$2"; then
    pid="$(cat "${home}/runs/$1/jobs/$2.pid")"
    # the runner leads its own session, see start
    pkill -TERM -s "${pid}" 2>/dev/null
    for i in 1 2 3 4; do runner_alive "$1" "$2" || break; sleep 0.5; done
    pkill -KILL -s "${pid}" 2>/dev/null
    for i in $(seq 20); do runner_alive "$1" "$2" || break; sleep 0.5; done
    if runner_alive "$1" "$2"; then echo "the runner of $2 is still running" >&2; return 1; fi
  fi
  stop_cell_containers "$1" "$2"
}

live_cells() {
  local n=0 p cell
  for p in "${home}/runs/$1/jobs/"*.pid; do
    [ -f "${p}" ] || continue
    cell="$(basename "${p}" .pid)"
    runner_alive "$1" "${cell}" && n=$((n + 1))
  done
  echo "${n}"
}

# exits 76 unless the label is the owner's, or force
check_owner() {
  local current
  current="$(cat "${home}/runs/$1/owner" 2>/dev/null)"
  if [ -n "${current}" ] && [ "${current}" != "$2" ] && [ "${3:-}" != "force" ]; then
    echo "label $1 belongs to another driver (${current})" >&2
    exit 76
  fi
}

# true when runs/<label> has the owner marker of a driver of this tool
owned_by_this_tool() {
  [[ "$(cat "${home}/runs/$1/owner" 2>/dev/null)" =~ ^[0-9a-f]{16}$ ]]
}

write_file() {
  echo "$2" > "$1.tmp.$$" && mv -f "$1.tmp.$$" "$1"
}

# serialises claim, put, mark, start, stop, clean, kill, labels and rmlabel of a label; the lock lives outside the
# label's directory, which rmlabel removes
lock_label() {
  mkdir -p "${home}/locks"
  exec {label_lock}>"${home}/locks/label-$1.lock"
  flock "${label_lock}"
}

# true when the attempt is the cell's latest
latest_attempt() {
  [ "$(cat "${home}/runs/$1/jobs/$2.attempt" 2>/dev/null)" == "$3" ]
}

# keeps the attempt from starting later
retire_attempt() {
  grep -qxF "$3" "${home}/runs/$1/jobs/$2.retired" 2>/dev/null || echo "$3" >> "${home}/runs/$1/jobs/$2.retired"
}

# the -t argument the checkout's .build/docker/run-tests.sh gives the container for a regexp: the line that makes it,
# run on its own
quoted_by_run_tests() {
  local line
  line="$(grep -m1 '^\[ -n "\${test_name_regexp}" \] && test_name_regexp_arg=' .build/docker/run-tests.sh)" || return 0
  test_name_regexp="$1" bash -c "${line}"'; echo "${test_name_regexp_arg}"' 2>/dev/null
}

# stops every cell of the label; fails when one of them could not be stopped
stop_label() {
  local f cell failed=0
  for f in "${home}/runs/$1/jobs/"*.pid "${home}/runs/$1/jobs/"*.runtime; do
    [ -f "${f}" ] || continue
    cell="$(basename "${f}")"
    stop_cell "$1" "${cell%.*}" || failed=1
  done
  return "${failed}"
}

# runs/<label> is a directory in runs/ that this tool made: else exits, 76 when it has no owner marker, 79 when it is
# a link or leads out of runs/
check_label_dir() {
  [ ! -L "${home}/runs/$1" ] || { echo "runs/$1 is a symbolic link, which run-ci-local does not use" >&2; exit 79; }
  target="$(cd "${home}/runs/$1" && pwd -P)"
  [ "$(dirname "${target}")" == "$(cd "${home}/runs" && pwd -P)" ] \
    || { echo "runs/$1 is ${target}, not a directory in runs/" >&2; exit 79; }
  owned_by_this_tool "$1" || { echo "runs/$1 exists, and was not made by run-ci-local" >&2; exit 76; }
}

# exits 78 unless the session is the label's
check_session() {
  if [ "$(cat "${home}/runs/$1/session" 2>/dev/null)" != "$2" ]; then
    echo "$1 has been taken over by another session of its driver" >&2
    exit 78
  fi
}

cmd="${1:-}"; shift || true
case "${cmd}" in
  init)
    [ -z "${1:-}" ] || [[ "$1" =~ ^[1-9][0-9]*$ ]] || { echo "invalid capacity '$1'" >&2; exit 2; }
    exec {alloc}>"${slots}/alloc.lock"; flock "${alloc}"
    [ -f "${home}/capacity" ] || write_file "${home}/capacity" "${1:-$(nproc)}"
    exit 0
    ;;
  set-capacity)
    [[ "${1:-}" =~ ^[1-9][0-9]*$ ]] || { echo "invalid capacity '${1:-}'" >&2; exit 2; }
    exec {alloc}>"${slots}/alloc.lock"; flock "${alloc}"
    write_file "${home}/capacity" "$1"
    exit 0
    ;;
  preflight)
    # problems no retry mends, and problems that may go away: a stopped daemon, a failed image pull
    problems=()
    passing=()
    for tool in flock xz bc tail timeout setsid pkill; do
      command -v "${tool}" >/dev/null 2>&1 || problems+=("${tool} is missing")
    done
    # _create_user.sh makes the container user with the host user's uid, which cannot be root's
    [ "$(id -u)" != "0" ] || problems+=("runs as root: the .build/docker scripts need a host user other than root")
    detect_runtime
    if [ "${runtime}" == "none" ]; then
      problems+=("neither docker nor podman is installed")
    elif ! "${runtime}" info >/dev/null 2>&1; then
      passing+=("\`${runtime} info\` fails: is the ${runtime} daemon or service running?")
    elif ! "${runtime}" image inspect alpine:3.19.1 >/dev/null 2>&1 \
        && ! pull="$("${runtime}" pull alpine:3.19.1 2>&1)"; then
      passing+=("cannot pull alpine:3.19.1: ${pull}")
    else
      # a container writes to a bind mount of the run directory, as the .build/docker scripts' containers do
      userns=()
      [ "${rootless}" == "yes" ] && userns=(--userns=keep-id)
      probe_file=".preflight-$$-${RANDOM}"
      if ! probe="$("${runtime}" run --rm "${userns[@]}" -v "${home}/runs":/probe alpine:3.19.1 \
            sh -c "touch /probe/${probe_file} && rm /probe/${probe_file}" 2>&1)"; then
        problems+=("a container cannot write to a bind mount of ${home}/runs, which the .build/docker scripts need" \
                   "(they mount without SELinux relabelling, so an enforcing SELinux refuses it): ${probe}")
      fi
    fi
    if [ "${#problems[@]}" -gt 0 ]; then
      printf '%s\n' "${problems[@]}" "${passing[@]}"
      exit 1
    fi
    [ "${#passing[@]}" -eq 0 ] && exit 0
    printf '%s\n' "${passing[@]}"
    exit 3
    ;;
  capacity)
    exec {alloc}>"${slots}/alloc.lock"; flock "${alloc}"
    echo "$(free_slots) $(capacity)"
    exit 0
    ;;
  runtime)
    detect_runtime
    echo "${runtime}$([ "${rootless}" == "yes" ] && echo " rootless")"
    exit 0
    ;;
  arch)
    case "$(uname -m)" in
      x86_64|amd64) echo amd64 ;;
      aarch64|arm64) echo arm64 ;;
      *) uname -m ;;
    esac
    exit 0
    ;;
  owner)
    [ $# -ge 2 ] || usage
    check_names "$1" "$2"
    [ -e "${home}/runs/$1" ] || [ -L "${home}/runs/$1" ] || exit 0
    check_label_dir "$1"
    check_owner "$1" "$2"
    exit 0
    ;;
  claim)
    [ $# -ge 3 ] || usage
    check_names "$1" "$2" "$3"
    lock_label "$1"
    [ ! -L "${home}/runs/$1" ] || check_label_dir "$1"
    if [ ! -e "${home}/runs/$1" ]; then
      # made whole aside and renamed into place, so that no claim leaves a directory without its owner; under the
      # label lock, what an interrupted claim left aside is stale
      claim_dir="${home}/runs/.claim-$1"
      rm -rf "${claim_dir}"
      mkdir -p "${claim_dir}/jobs" && echo "$2" > "${claim_dir}/owner" && echo "$3" > "${claim_dir}/session" \
        && mv -T "${claim_dir}" "${home}/runs/$1" || { rm -rf "${claim_dir}"; exit 1; }
    fi
    owned_by_this_tool "$1" || { echo "runs/$1 exists, and was not made by run-ci-local" >&2; exit 76; }
    check_label_dir "$1"
    check_owner "$1" "$2"
    # only the stop that takes a label over changes its session
    check_session "$1" "$3"
    exit 0
    ;;
  unreserve)
    [ $# -ge 2 ] || usage
    check_names "$1" "$2"
    rm -f "${home}/reservations/$1.$2"
    exit 0
    ;;
  status)
    [ $# -ge 3 ] || usage
    check_names "$1" "$2" "$3"
    jd="${home}/runs/$1/jobs/$2"
    latest_attempt "$1" "$2" "$3" || { echo "unknown"; exit 0; }
    if [ -f "${jd}.rc" ]; then echo "done $(cat "${jd}.rc")"; exit 0; fi
    if runner_alive "$1" "$2"; then echo "running"; exit 0; fi
    # the pid file is written just after the runner starts
    if [ -f "${jd}.started" ] && [ ! -f "${jd}.pid" ] \
        && [ $(( $(date +%s) - $(stat -c %Y "${jd}.started") )) -lt 30 ]; then
      echo "running"; exit 0
    fi
    echo "lost"
    exit 0
    ;;
  stop)
    [ $# -ge 3 ] || usage
    check_names "$1" "$2" "$3"
    [ -d "${home}/runs/$1/jobs" ] || { echo "not running"; exit 0; }
    lock_label "$1"
    retire_attempt "$1" "$2" "$3"
    if ! latest_attempt "$1" "$2" "$3"; then
      echo "not running"
    elif [ -f "${home}/runs/$1/jobs/$2.rc" ]; then
      # the runner stops what it leaves running as it ends; that is confirmed here
      stop_cell_containers "$1" "$2" || exit 1
      echo "finished"
    elif runner_alive "$1" "$2"; then
      stop_cell "$1" "$2" || exit 1
      # a runner that wrote its result as it was stopped had ended
      if [ -f "${home}/runs/$1/jobs/$2.rc" ]; then echo "finished"; else echo "stopped"; fi
    else
      stop_cell_containers "$1" "$2" || exit 1
      echo "not running"
    fi
    exit 0
    ;;
  clean)
    [ $# -ge 4 ] || usage
    check_names "$1" "$2" "$3" "$4"
    [ -d "${home}/runs/$1/jobs" ] || exit 0
    lock_label "$1"
    check_session "$1" "$4"
    latest_attempt "$1" "$2" "$3" || exit 0
    jd="${home}/runs/$1/jobs/$2"
    rm -f "${jd}.rc" "${jd}.pid" "${jd}.started" "${jd}.result.tar" "${jd}.env" "${jd}.runner.log" \
          "${jd}.runtime" "${jd}.containers"
    exit $?
    ;;
  labels)
    cd "${home}/runs" || exit 0
    for d in */; do
      label="${d%/}"
      [ ! -L "${label}" ] && owned_by_this_tool "${label}" || continue
      newest=$(find "${label}" -maxdepth 2 -printf '%T@\n' 2>/dev/null | sort -n | tail -1)
      bytes=$(find "${label}" -maxdepth 1 -name '*.tar*' -printf '%s\n' 2>/dev/null \
                | awk '{ s += $1 } END { print s + 0 }')
      # under the label lock, a cell that is starting counts as running
      lock_label "${label}"
      echo "${label} $(live_cells "${label}") ${newest%%.*} ${bytes} $(cat "${label}/owner")"
      exec {label_lock}>&-
    done
    exit 0
    ;;
  kill)
    [ $# -ge 2 ] || usage
    check_names "$1" "$2"
    [ -z "${3:-}" ] || check_names "$3"
    lock_label "$1"
    [ -e "${home}/runs/$1" ] || exit 0
    check_label_dir "$1"
    check_owner "$1" "$2"
    [ -n "${3:-}" ] && write_file "${home}/runs/$1/session" "$3"
    running="$(live_cells "$1")"
    stop_label "$1" || exit 1
    # what the stopped cells and an earlier session's writes left is not a result
    rm -f "${home}/runs/$1/".put-*
    for f in "${home}/runs/$1/jobs/"*; do
      [ -d "${f}" ] && remove_tree "${f}"
      [ -f "${f}" ] && [[ "${f}" != *.retired ]] && [[ "${f}" != *.attempt ]] && rm -f "${f}"
    done
    echo "${running}"
    exit 0
    ;;
  rmlabel)
    [ $# -ge 2 ] || usage
    check_names "$1" "$2"
    lock_label "$1"
    rm -rf "${home}/runs/.claim-$1"
    [ -e "${home}/runs/$1" ] || exit 0
    check_label_dir "$1"
    check_owner "$1" "$2" "${3:-}"
    if [ "$(live_cells "$1")" != "0" ]; then echo "$1 still has running cells" >&2; exit 1; fi
    remove_tree "${target}"
    exit $?
    ;;
  put)
    [ $# -ge 4 ] || usage
    label="$1"; session="$2"; path="$3"; bytes="$4"; version="${5:-}"
    check_names "${label}" "${session}" "${path#jobs/}" ${version:+"${version}"}
    [[ "${bytes}" =~ ^[0-9]+$ ]] || { echo "invalid size '${bytes}'" >&2; exit 2; }
    [ -d "${home}/runs/${label}/jobs" ] || { echo "runs/${label} has not been claimed" >&2; exit 2; }
    # written aside, then put in place under the label lock while the session is the label's
    part="${home}/runs/${label}/.put-$$"
    cat > "${part}" || { rm -f "${part}"; exit 1; }
    arrived="$(stat -c %s "${part}")"
    if [ "${arrived}" != "${bytes}" ]; then
      rm -f "${part}"
      echo "${path}: ${arrived} bytes of ${bytes} arrived" >&2
      exit 1
    fi
    lock_label "${label}"
    [ "$(cat "${home}/runs/${label}/session" 2>/dev/null)" == "${session}" ] || rm -f "${part}"
    check_session "${label}" "${session}"
    # no moment where the version describes another file
    rm -f "${home}/runs/${label}/${path}.version" || exit 1
    mv -f "${part}" "${home}/runs/${label}/${path}" || exit 1
    [ -z "${version}" ] || write_file "${home}/runs/${label}/${path}.version" "${version}"
    exit $?
    ;;
  mark)
    [ $# -ge 4 ] || usage
    check_names "$1" "$2" "$3" "$4"
    lock_label "$1"
    check_session "$1" "$2"
    [ -f "${home}/runs/$1/$3" ] || { echo "runs/$1/$3 is missing" >&2; exit 1; }
    write_file "${home}/runs/$1/$3.version" "$4"
    exit $?
    ;;
  start)
    [ $# -ge 6 ] || usage
    label="$1"; cell="$2"; units="$3"; owner="$4"; attempt="$5"; session="$6"; mode="${7:-}"
    check_names "${label}" "${cell}" "${owner}" "${attempt}" "${session}"
    [[ "${units}" =~ ^[0-9]+$ ]] || { echo "invalid units '${units}'" >&2; exit 2; }
    # held until the runner's pid is written, so that kill, labels and rmlabel see a starting cell as running
    lock_label "${label}"
    check_owner "${label}" "${owner}"
    check_session "${label}" "${session}"
    if grep -qxF "${attempt}" "${home}/runs/${label}/jobs/${cell}.retired" 2>/dev/null; then
      echo "attempt ${attempt} of ${cell} was stopped or replaced" >&2
      exit 78
    fi
    if latest_attempt "${label}" "${cell}" "${attempt}"; then
      echo "attempt ${attempt} of ${cell} has started already"
      exit 0
    fi
    if runner_alive "${label}" "${cell}"; then
      echo "an earlier attempt of ${cell} still runs" >&2
      exit 77
    fi
    # before anything of the earlier attempt is removed
    if ! stop_cell_containers "${label}" "${cell}"; then
      echo "the containers of an earlier attempt of ${cell} may still run" >&2
      exit 77
    fi
    cap="$(capacity)"
    [ "${units}" -le "${cap}" ] || units="${cap}"
    jd="${home}/runs/${label}/jobs/${cell}"
    [ -f "${jd}.env" ] || { echo "missing ${jd}.env" >&2; exit 2; }
    exec {alloc}>"${slots}/alloc.lock"; flock "${alloc}"
    # A cell that has waited long enough passes "reserve": its units are recorded here, and other cells start only
    # in the units left after every other reservation.  A reservation not renewed for 3 minutes is dropped.
    reservations="${home}/reservations"
    mkdir -p "${reservations}"
    find "${reservations}" -type f -mmin +3 -delete 2>/dev/null
    own="${reservations}/${label}.${cell}"
    reserved=0
    for f in "${reservations}"/*; do
      [ -f "${f}" ] && [ "${f}" != "${own}" ] && reserved=$(( reserved + $(cat "${f}" 2>/dev/null || echo 0) ))
    done
    if [ $(( $(free_slots) - reserved )) -lt "${units}" ]; then
      # reservations beyond the capacity would block each other
      [ "${mode}" == "reserve" ] && [ $(( reserved + units )) -le "${cap}" ] && echo "${units}" > "${own}"
      exit 75
    fi
    rm -f "${own}"
    got=()
    for ((i = 0; i < cap && ${#got[@]} < units; i++)); do
      exec {fd}>"${slots}/slot.${i}"
      if flock -n "${fd}"; then got+=("${fd}"); else exec {fd}>&-; fi
    done
    if [ "${#got[@]}" -lt "${units}" ]; then
      for fd in "${got[@]}"; do exec {fd}>&-; done
      exit 75
    fi
    previous="$(cat "${jd}.attempt" 2>/dev/null)"
    [ -n "${previous}" ] && retire_attempt "${label}" "${cell}" "${previous}"
    write_file "${jd}.attempt" "${attempt}"
    rm -f "${jd}.rc" "${jd}.result.tar" "${jd}.pid" "${jd}.containers"
    touch "${jd}.started"
    # the runner inherits the slot locks, but not the allocation and label locks
    setsid nohup "$0" _run "${label}" "${cell}" "${units}" "${got[*]}" "${attempt}" \
      > "${jd}.runner.log" 2>&1 < /dev/null {alloc}>&- {label_lock}>&- &
    echo $! > "${jd}.pid.tmp" && mv -f "${jd}.pid.tmp" "${jd}.pid"
    exit 0
    ;;
  _run)
    ;;
  *)
    usage
    ;;
esac

################################
#
# _run: one cell, detached
#
################################

label="$1"; cell="$2"; units="$3"; slot_fds="$4"; attempt="$5"
# A holder process keeps the slot locks until this runner exits, and the runner closes its own copies, so that
# nothing the cell starts, such as podman's long-lived helper processes, inherits them and outlives the cell.
tail --pid=$$ -f /dev/null 2>/dev/null &
for fd in ${slot_fds}; do eval "exec ${fd}>&-"; done

run_dir="${home}/runs/${label}"
jd="${run_dir}/jobs/${cell}"
# KIND STEP SCRIPT JDK ARCH SPLIT SPLITS PYTHON CYTHON TEST_REGEXP TIMEOUT_HOURS PYTHON_DTEST CELL_SUFFIX RUNTIME BASE,
# and for the report SUMMARY_BUILD_TAG and SUMMARY_<Jenkins parameter>
# shellcheck disable=SC1090
source "${jd}.env"
# the cell's axes, as withEnv in .jenkins/Jenkinsfile sets them
export arch="${ARCH}" jdk="${JDK}" python="${PYTHON}" cython="${CYTHON}" step="${STEP}" split="${SPLIT}"
[ -n "${RUNTIME}" ] && export CI_LOCAL_RUNTIME="${RUNTIME}"
start_ts=$(date +%s)
ws="${jd}/cassandra"
remove_tree "${jd}"
mkdir -p "${ws}" "${jd}/out"
stage_log="${jd}/out/stage.log"
echo "run-ci-local: ${cell} on $(hostname) units=${units} start=$(date -u +%FT%TZ)" > "${stage_log}"

# a runner whose attempt is no longer the latest leaves the cell's files to the latest
finish() {
  cd /
  latest_attempt "${label}" "${cell}" "${attempt}" || exit 0
  echo "$1" > "${jd}/out/rc"
  tar -C "${jd}" -cf "${jd}.result.tar.tmp" out && mv "${jd}.result.tar.tmp" "${jd}.result.tar"
  remove_tree "${jd}"
  write_file "${jd}.rc" "$1"
  exit 0
}

if [ "${KIND}" == "jar" ] || [ "${KIND}" == "report" ]; then
  tar -xf "${run_dir}/src.tar" -C "${ws}" \
    || { echo "run-ci-local: cannot extract src.tar" >> "${stage_log}"; finish 97; }
else
  tar -xf "${run_dir}/${BASE}" -C "${ws}" \
    || { echo "run-ci-local: cannot extract ${BASE}" >> "${stage_log}"; finish 97; }
fi
cd "${ws}" || finish 97
mkdir -p build/stage-logs build/test/output build/test/logs

detect_runtime
echo "${runtime}" > "${jd}.runtime"
echo "run-ci-local: container runtime ${runtime}$([ "${rootless}" == "yes" ] && echo " (rootless)")" >> "${stage_log}"
# a cell never asks for more cpus than the host has
host_cpus="$(nproc)"
cpus=$(( units < host_cpus ? units : host_cpus ))
if [ "${runtime}" == "podman" ]; then
  # .build/docker/*.sh call `docker`: the podman/docker wrapper next to this script translates
  export PATH="${bin_dir}/podman:${PATH}"
  export CI_LOCAL_HOME="${home}"
  export CI_LOCAL_PODMAN_ROOTLESS="${rootless}"
  export CI_LOCAL_CPUS="${cpus}"
  export CI_LOCAL_CONTAINERS="${jd}.containers"
fi
# containers otherwise get every core of the host, see .build/docker/run-tests.sh
export docker_cpus="${cpus}"
export m2_dir="${ws}/build/m2"
export SKIP_DOCKER_DEBUG_ON_FAIL=1
timeout_s=$(( TIMEOUT_HOURS * 3600 ))

case "${KIND}" in
  jar|build)
    # as build() in .jenkins/Jenkinsfile: `<script> <jdk>`, where the script may carry arguments
    # shellcheck disable=SC2086
    timeout -k 60 "${timeout_s}" .build/docker/${SCRIPT} "${JDK}" >> "${stage_log}" 2>&1
    rc=$?
    if [ "${KIND}" == "jar" ] && [ "${rc}" == "0" ]; then
      # the stash every other cell of this jdk starts from.  build/m2 is per container, and refilled from the image
      tar -C "${ws}" --exclude=./build/m2 --exclude=./build/tmp -cf "${run_dir}/${BASE}.tmp" . \
        && rm -f "${run_dir}/${BASE}.version" && mv "${run_dir}/${BASE}.tmp" "${run_dir}/${BASE}" || rc=96
    fi
    ;;
  test)
    # as test() in .jenkins/Jenkinsfile, see testScriptVars and buildJVMDTestJars there
    export python_version="${PYTHON}"
    if [ "${PYTHON_DTEST}" == "yes" ]; then
      mkdir -p build/cassandra-dtest
      tar -xf "${run_dir}/dtest.tar" -C build/cassandra-dtest \
        || { echo "run-ci-local: cannot extract dtest.tar" >> "${stage_log}"; finish 97; }
      export cassandra_dtest_dir="${ws}/build/cassandra-dtest"
    fi
    args=(-a "${STEP}" -j "${JDK}")
    if [ -z "${TEST_REGEXP}" ]; then
      args+=(-c "${SPLIT}/${SPLITS}")
    elif [ "$(quoted_by_run_tests 'a|b')" == '-t a\|b' ]; then
      args+=(-t "${TEST_REGEXP}")
    else
      # an older run-tests.sh puts the regexp unquoted into the command line a shell in the container runs
      args+=(-t "$(printf '%q' "${TEST_REGEXP}")")
    fi
    timeout -k 60 "${timeout_s}" bash -c '
        step="$1" jdk="$2" hours="$3" ; shift 3
        if [[ "${step}" == jvm-dtest-upgrade* ]] ; then
          .build/docker/run-tests.sh -a build_dtest_jars -j "${jdk}" || exit $?
        fi
        docker_timeout_hours="${hours}" .build/docker/run-tests.sh "$@"' \
      run-tests "${STEP}" "${JDK}" "${TIMEOUT_HOURS}" "${args[@]}" >> "${stage_log}" 2>&1
    rc=$?
    ;;
  report)
    # as generateTestReports() in .jenkins/Jenkinsfile, with the build tag and the Jenkins parameters, in the lower
    # case Jenkins sets them, for ci_summary.html
    export BUILD_TAG="${SUMMARY_BUILD_TAG}" repository="${SUMMARY_repository}" branch="${SUMMARY_branch}" \
           profile="${SUMMARY_profile}" profile_custom_regexp="${SUMMARY_profile_custom_regexp}" \
           architecture="${SUMMARY_architecture}" jdk="${SUMMARY_jdk}" dtest_repository="${SUMMARY_dtest_repository}" \
           dtest_branch="${SUMMARY_dtest_branch}"
    tar -xf "${run_dir}/report-input.tar" -C build \
      || { echo "run-ci-local: cannot extract report-input.tar" >> "${stage_log}"; finish 97; }
    # as there, a target whose report fails does not stop the others
    timeout -k 60 "${timeout_s}" bash -c '
        find build/test/output -type f -name "*.xml.xz" -print0 | xargs -0 -r -n64 xz -f --decompress
        for target in $(ls build/test/output/) ; do
          mkdir -p "build/test/reports/${target}"
          CASSANDRA_DOCKER_ANT_OPTS="-Xmx2g -Dbuild.test.output.dir=build/test/output/${target}"
          CASSANDRA_DOCKER_ANT_OPTS+=" -Dbuild.test.report.dir=build/test/reports/${target}"
          CASSANDRA_DOCKER_ANT_OPTS="${CASSANDRA_DOCKER_ANT_OPTS}" \
            .build/docker/_docker_run.sh debian-build.docker ci/generate-test-report.sh \
            || echo "failed generate-test-report.sh of ${target}"
        done
        # the summary is what the report is for: unlike in Jenkins, its failure fails the report
        .build/docker/_docker_run.sh debian-build.docker ci/generate-ci-summary.sh || exit $?
        tar -cf build/results_details.tar -C build/test/ reports && xz -8f build/results_details.tar
        ' >> "${stage_log}" 2>&1
    rc=$?
    cp build/ci_summary.html build/results_details.tar.xz "${jd}/out/" 2>/dev/null
    ;;
  *)
    echo "run-ci-local: unknown kind ${KIND}" >> "${stage_log}"
    rc=2
    ;;
esac
end_ts=$(date +%s)
echo "run-ci-local: end rc=${rc} duration_s=$((end_ts - start_ts)) end=$(date -u +%FT%TZ)" >> "${stage_log}"
if [ "${rc}" == "124" ] || [ "${rc}" == "137" ]; then
  echo "run-ci-local: TIMEOUT after ${timeout_s}s" >> "${stage_log}"
fi
echo "$((end_ts - start_ts))" > "${jd}/out/duration_s"
# a timed out cell leaves its container running
latest_attempt "${label}" "${cell}" "${attempt}" && stop_cell_containers "${label}" "${cell}"

if [ "${KIND}" == "test" ]; then
  # as organiseTestResultFiles() in .jenkins/Jenkinsfile
  (
    cd build || exit 0
    mkdir -p "test/output/${STEP}"
    find test/output -type f -name "TEST*.xml" -execdir mkdir -p "jdk_${JDK}/${ARCH}" ';' \
      -execdir mv {} "jdk_${JDK}/${ARCH}/{}" ';'
    find test/output -name cqlshlib.xml -execdir mv cqlshlib.xml "${STEP}/cqlshlib${CELL_SUFFIX}.xml" ';'
    find test/output -name nosetests.xml -execdir mv nosetests.xml "${STEP}/nosetests${CELL_SUFFIX}.xml" ';'
  )
  cp -a build/test/output "${jd}/out/output" 2>/dev/null
  mkdir -p "${jd}/out/logs"
  find build/test/logs -maxdepth 1 -type f \( -name 'docker_attach_*' -o -name 'memory_events_*' \) \
    -exec cp {} "${jd}/out/logs/" \; 2>/dev/null
  # test logs, up to 2 GiB of them
  if du -sk build/test/logs 2>/dev/null | awk '{ exit ($1 > 2097152) }'; then
    tar -C build/test -cf - logs 2>/dev/null | xz -T2 -c > "${jd}/out/logs/test-logs.tar.xz"
  fi
elif [ "${KIND}" == "build" ]; then
  find build -maxdepth 1 -type f \( -name 'apache-cassandra-*' -o -name 'cassandra_*' -o -name 'cassandra-tools_*' \
    -o -name '*.rpm' -o -name '*.deb' \) -printf '%f %s\n' > "${jd}/out/artifacts.txt" 2>/dev/null
fi
finish "${rc}"
