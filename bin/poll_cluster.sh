#!/bin/bash
#
# Copyright © 2017 Cask Data, Inc.
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

# This script can be used to poll a Coopr cluster until it's active

export COOPR_SERVER_URI=${COOPR_SERVER_URI:-http://localhost:55054}
export COOPR_TENANT=${COOPR_TENANT:-superadmin}
export COOPR_API_USER=${COOPR_API_USER:-admin}

die() { echo "ERROR: ${*}"; exit 1; };

cluster_id() { echo $(<"${COOPR_DRIVER_CLUSTER_ID_FILE}"); };

parse_args() {
  local __opts
  for __opts in ${@}; do
    case ${__opts} in
      --cluster-id-file)
        shift
        if [[ ${1} =~ /^-/ ]] || [[ -z ${1} ]]; then
          die "Missing argument to --cluster-id-file"
        fi
        if [[ -r ${1} ]] && [[ -f ${1} ]]; then
          COOPR_DRIVER_CLUSTER_ID_FILE=${1}
        else
          die "Invalid argument to --cluster-id-file: ${1}"
        fi
        shift
        ;;
      -i|--cluster-id)
        shift
        if [[ ${1} =~ /^-/ ]] || [[ -z ${1} ]]; then
          die "Missing argument to --cluster-id"
        fi
        export __id=${1}
        shift
        ;;
    esac
  done
  if [[ -n ${COOPR_DRIVER_CLUSTER_ID_FILE} ]]; then
    export __id=$(cluster_id)
  fi
}

_status() {
  curl -sSL \
    -XGET \
    -HCoopr-UserId:${COOPR_API_USER} \
    -HCoopr-TenantId:${COOPR_TENANT} \
    ${COOPR_SERVER_URI}/v2/clusters/${__id}/status | python -mjson.tool 2>/dev/null
}

# Polls for cluster status, returns true if cluster active
active() {
  echo -n $(_status) | grep active >/dev/null
}

poll_until_active() {
  echo -n "Polling for completion"
  while true; do
    active
    if [[ $? -eq 0 ]]; then
      break
    fi
    if [[ $(echo -n $(_status) | grep pending >/dev/null) ]]; then
      echo -n .
    else
      echo
      die "Cluster ${__id} is not in an active or pending state"
    fi
    sleep 30
  done
  echo
  echo "Cluster ${__id} is complete and active"
}

main() {
  parse_args ${@}
  poll_until_active
}

main ${@}
