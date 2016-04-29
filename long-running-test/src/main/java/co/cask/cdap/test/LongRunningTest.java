/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.test;

/**
 * Represents a long running CDAP test.
 *
 * @param <T> Type of test state
 */
public interface LongRunningTest<T extends TestState> {
  /**
   * Used to deploy application.
   * Will be called before #start.
   */
  void deploy() throws Exception;

  /***
   * Used to start programs. It is guaranteed that #deploy is called before this.
   */
  void start() throws Exception;

  /**
   * Called after a run of the test.
   * Used to stop apps, and run other clean up operations.
   * The goal of this method is to get the cluster in a resettable state.
   */
  void stop() throws Exception;

  /**
   * Called when saved state for the test is not found (during the first run of the test)
   *
   * @return initial state object
   */
  T getInitialState();

  /**
   * This can be used to ensure that all data operations performed in the
   * previous call to #runOperations are complete.
   */
  void awaitOperations(T state) throws Exception;

  /**
   * Called during every run of the test.
   * Data operations performed in previous runs can be verified using the saved state.
   * #runOperations will be called after this method.
   * If this method throws exception then #runOperations will not be called, and test will fail.
   * </p>
   *
   * @param state state saved from previous call to #runOperations
   */
  void verifyRuns(T state) throws Exception;

  /**
   * Called during every run of the test after #verifyRuns.
   * All data operations for a test run are performed here.
   * This method will not be called if #verifyRuns throws exception.
   *
   * @param state state saved from previous call to this method
   * @return state of current run
   */
  T runOperations(T state) throws Exception;
}
