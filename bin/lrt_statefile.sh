#!/bin/bash
#
# Copyright © 2016 Cask Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

# Usage $0 $ip <get|put>

REMOTE_STATE_FILE=${REMOTE_STATE_FILE:-~/long-running-test.state}
IN_STATE_FILE=${IN_STATE_FILE:-long-running-test/long-running-test-in.state}
OUT_STATE_FILE=${OUT_STATE_FILE:-long-running-test/long-running-test-out.state}

REMOTE_HOST=${REMOTE_HOST:-${1}}
__action=${2:-get}
__ssh_options="-oStrictHostKeyChecking=no -oUserKnownHostsFile=/dev/null -oLogLevel=ERROR"

__resolve() {
  host ${1} | grep -v IPv6 | head -n 1 | awk '{print $4}'
  return $?
}

die() { echo "ERROR: ${@}"; exit 1; }

if [[ ${REMOTE_HOST} =~ ^[[:digit:]] ]]; then
  __ip=$(resolve ${REMOTE_HOST}) || die "Cannot resolve ${REMOTE_HOST}"
else
  __ip=${REMOTE_HOST}
fi

case ${__action} in
  get)
    ssh ${__ssh_options} ${__ip} "test -f ${REMOTE_STATE_FILE}"
    if [[ $? -ne 0 ]]; then
      echo "Remote state file ${REMOTE_STATE_FILE} not present on ${REMOTE_HOST}"
      exit 0
    else
      scp ${__ssh_options} ${__ip}:${REMOTE_STATE_FILE} ${IN_STATE_FILE}
      exit $?
    fi
    ;;
  put) scp ${__ssh_options} ${OUT_STATE_FILE} ${__ip}:${REMOTE_STATE_FILE}; exit $? ;;
  *) die "Unrecognized parameter" ;;
esac

# we should never get here
exit 0
