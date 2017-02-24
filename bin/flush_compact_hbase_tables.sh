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

# This script when executed, runs in a loop and flushes and compacts all cdap system tables

while true
do
  echo "realtime" | kinit hbase
  for i in `echo "list" | hbase shell | grep 'cdap_system' | fgrep -v '['`; do echo "flush '$i'"; echo "major_compact '$i'"; done | hbase shell
  sleep ${1:-600}
done
