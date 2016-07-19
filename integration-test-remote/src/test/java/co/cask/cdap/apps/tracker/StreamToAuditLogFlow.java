/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.apps.tracker;

import co.cask.cdap.api.flow.AbstractFlow;
import co.cask.tracker.AuditLogPublisher;

/**
 * This class is used to create a test flow connecting the Generator to the AuditLog Flowlet.

 */
public class StreamToAuditLogFlow extends AbstractFlow {
  public static final String FLOW_NAME = "StreamToAuditLogFlow";

  @Override
  public void configure() {
    setName(FLOW_NAME);
    setDescription("A temp flow to test the audit log");
    addFlowlet("auditLogPublisher", new AuditLogPublisher());
    connectStream("testStream", "auditLogPublisher");
  }
}
