/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.cdap.plugins;

import io.cdap.chaosmonkey.Disruption;
import io.cdap.chaosmonkey.RemoteProcess;
import io.cdap.chaosmonkey.ShellOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

/**
 * A disruption that performs a major compaction on hbase
 *
 * Example:
 * clusterDisruptor.disruptAndWait("hbase-master", "major-compact", actionArguments, timeoutSeconds, TimeUnit.Seconds);
 */
public class MajorCompact implements Disruption {
  private static final Logger LOG = LoggerFactory.getLogger(MajorCompact.class);

  @Override
  public void disrupt(Collection<RemoteProcess> processes, Map<String, String> serviceArguments) throws Exception {
    LOG.info("Major compaction invoked...");
    RemoteProcess remoteProcess = processes.iterator().next();

    String user = serviceArguments.get("user");
    if (user == null) {
      user = "hbase";
    }

    String keytabPath = serviceArguments.get("keytab.path");
    if (keytabPath == null) {
      keytabPath = "/etc/security/keytabs/hbase.service.keytab";
    }

    String authEnabled = serviceArguments.get("auth.enabled");
    String authString = String.format("sudo -su %s kinit -kt %s hbase/`hostname -f`;", user, keytabPath);
    if (authEnabled != null && authEnabled.equals("false")) {
      authString = "";
    }
    String command = String.format("%s echo \"list\" | sudo -u %s hbase " +
                                    "shell | fgrep '['", authString, user);
    LOG.info("Running Command = " + command);
    ShellOutput output = remoteProcess.execAndGetOutput(command);
    LOG.info("Return code = " + output.returnCode);
    LOG.info("Standard Output = " + output.standardOutput);
    LOG.info("Error Output = " + output.errorOutput);

    // stdout is in the format of ["tableName1" "tableName2" "tableName3"....] and needs to be changed to
    // flush '\''tableName1'\''\n major_compact '\''table1'\''\n ...
    String outputString = output.standardOutput.replace("\"", "").replace("[", "").replace("]", "")
      .replaceAll("\\s+", "");
    String[] outputArray = outputString.split(",");
    StringBuilder stringBuilder = new StringBuilder();
    for (String tableName : outputArray) {
      // '\\'' will result in a single quote when passed into hbase shell, echo will exclude the outer quotes and
      // include the escaped single quote
      stringBuilder.append("flush '\\''" + tableName + "'\\''\n");
      stringBuilder.append("major_compact '\\''" + tableName + "'\\''\n");
    }
    String commandsString = stringBuilder.toString();

    String commandToExec = String.format("%s echo \"%s\" | sudo -u %s hbase shell",
                                         authString, commandsString, user);
    LOG.info("Running Command = " + commandToExec);
    ShellOutput shellOutput = remoteProcess.execAndGetOutput(commandToExec);
    LOG.info("Return code = " + shellOutput.returnCode);
    LOG.info("Standard Output = " + shellOutput.standardOutput);
    LOG.info("Error Output = " + shellOutput.errorOutput);

    LOG.info("Major compaction done.");
  }

  @Override
  public String getName() {
    return "major-compact";
  }
}
