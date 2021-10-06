/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.app.etl.google;

import org.junit.BeforeClass;

import io.cdap.cdap.app.etl.ETLTestBase;

/**
 * An abstract class used for running integration tests with Google OAuth2 user account credentials.
 */
public abstract class UserCredentialsTestBase extends ETLTestBase {
  private static String clientId;
  private static String clientSecret;
  private static String refreshToken;

  @BeforeClass
  public static void userCredentialsSetup() {
    clientId = System.getProperty("google.application.clientId");
    clientSecret = System.getProperty("google.application.clientSecret");
    refreshToken = System.getProperty("google.application.refreshToken");
    if (clientId == null || clientSecret== null || refreshToken == null) {
      throw new IllegalArgumentException("Invalid user credential parameters");
    }
  }

  public static String getClientId() {
    return clientId;
  }

  public static String getClientSecret() {
    return clientSecret;
  }

  public static String getRefreshToken() {
    return refreshToken;
  }
}
