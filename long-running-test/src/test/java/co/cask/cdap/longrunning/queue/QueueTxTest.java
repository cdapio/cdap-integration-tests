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

package co.cask.cdap.longrunning.queue;

import co.cask.cdap.client.StreamClient;
import co.cask.cdap.longrunning.increment.IncrementApp;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.LongRunningTestBase;
import co.cask.cdap.test.TestState;

import java.util.concurrent.TimeUnit;

/**
 * Generates entries in the queue table with invalid transactions
 */
public class QueueTxTest extends LongRunningTestBase<QueueTxTest.IntState> {

  public static class IntState implements TestState {
    private int count = 0;
  }

  @Override
  public void deploy() throws Exception {
    deployApplication(getLongRunningNamespace(), QueueFailureApp.class);
  }

  @Override
  public void start() throws Exception {
  }

  @Override
  public void stop() throws Exception {
  }

  private ApplicationManager getApplicationManager() throws Exception {
    return getApplicationManager(getLongRunningNamespace().app(QueueFailureApp.NAME));
  }

  @Override
  public IntState getInitialState() {
    return new IntState();
  }

  @Override
  public void awaitOperations(IntState state) throws Exception {
    // no-op
  }

  @Override
  public void verifyRuns(IntState state) throws Exception {
    // no-op
  }

  @Override
  public IntState runOperations(IntState state) throws Exception {
    getApplicationManager().getFlowManager(QueueFailureApp.FailureFlow.NAME).start();

    StreamClient streamClient = getStreamClient();
    StreamId streamId = getLongRunningNamespace().stream(QueueFailureApp.FailConfig.STREAM);
    streamClient.sendEvent(streamId, "failnow");
    state.count++;
    return state;
  }
}
