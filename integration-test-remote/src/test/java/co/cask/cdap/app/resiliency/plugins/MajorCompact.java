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

package co.cask.cdap.app.resiliency.plugins;

import co.cask.chaosmonkey.Disruption;
import co.cask.chaosmonkey.RemoteProcess;
import co.cask.chaosmonkey.ShellOutput;
import com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * A disruption that performs a major compaction on hbase
 */
public class MajorCompact implements Disruption {
  private static final Logger LOG = LoggerFactory.getLogger(MajorCompact.class);

  @Override
  public void disrupt(Collection<RemoteProcess> collection, Map<String, String> map) throws Exception {
    RemoteProcess remoteProcess = collection.iterator().next();
    ShellOutput output = remoteProcess.execAndGetOutput("sudo kinit -kt /etc/security/keytabs/hbase.service.keytab" +
                                                          " hbase/`hostname -f`; echo \"list\" | sudo hbase shell " +
                                                          "| fgrep '['");

    String outputString = output.standardOutput.replace("\"", "").replace("[", "").replace("]","")
      .replaceAll("\\s+", "");
    String[] outputArray = outputString.split(",");
    List<String> commands = new ArrayList<>();
    for (int idx = 0; idx < outputArray.length; ++idx) {
      commands.add("flush '\\''" + outputArray[idx] + "'\\''");
      commands.add("major_compact '\\''" + outputArray[idx] + "'\\''");
    }
    String commandsString = Joiner.on("\n").join(commands);

    remoteProcess.execAndGetOutput(String.format("sudo kinit -kt /etc/security/keytabs/hbase.service.keytab hbase/" +
                                                   "`hostname -f`; echo \"%s\" | sudo hbase shell", commandsString));
  }

  @Override
  public String getName() {
    return "major-compact";
  }
}
