#!/bin/bash
#
# Copyright © 2015 Cask Data, Inc.
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

REPO_FILE=${REPO_FILE:-${1}}
DEST_HOST=${DEST_HOST:-${2}}

__scp_options="-oStrictHostKeyChecking=no -oUserKnownHostsFile=/dev/null -oLogLevel=ERROR"
__repo_filename=$(basename ${REPO_FILE})

scp ${__scp_options} ${REPO_FILE} ${DEST_HOST}:/tmp
ssh ${__scp_options} ${DEST_HOST} "mkdir -p /tmp/repo && tar xf /tmp/${__repo_filename} -C /tmp/repo"
