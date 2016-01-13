CDAP Integration Tests
======================

Writing Tests
-------------
New test cases should be written in the ``integration-test-remote`` module, alongside the existing test cases.
They can be within their own package, as long as it is a "sub-package" of ``co.cask.cdap.apps``.

When writing an integration test, the same APIs are available as in CDAP unit tests.
Note that some APIs are not available, such as the ``TestManager`` methods ``deployTemplate``, ``addTemplatePlugins``,
and ``getDataset``.

For instance, to compile and deploy an Application class and start a Flow of that application::

  ApplicationManager applicationManager = deployApplication(WordCount.class);
  applicationManager.getFlowManager("WordCounter").start()

If additional APIs are needed that are not available in the CDAP unit tests framework,
the CDAP clients library can be utilized to interact with the CDAP instance.

For instance, example usage of StreamClient is in the StreamTest test class. Other clients can be
instantiated in a similar manner::

  StreamClient streamClient = new StreamClient(getClientConfig(), getRestClient());
  streamClient.create("TestStream");


Running Tests
-------------
To run integration tests against a remote CDAP instance, execute::

  mvn clean test -DinstanceUri=<HostAndPort>

Note that the CDAP instance against which the integration tests are run must have unrecoverable reset enabled.

To run integration tests against an automatically instantiated CDAP Standalone instance, execute::

  mvn clean test -P standalone-test


CDAP Upgrade Tests
------------------
The upgrade test cases consist of a ``pre`` stage and a ``post`` stage. The pre stage is test code which
is designed to set up the CDAP instance with applications and data. The post stage is then intended to run after
upgrading the CDAP instance to assert that the upgrade was successful.

To run upgrade tests, execute the following steps::

  1. Run the pre stage against an older version of CDAP.
  2. Upgrade the CDAP instance to the newer version.
  3. Run the post stage against an newer version of CDAP.


To run a particular stage, execute the following from the commandline::

  mvn test -P upgrade-test-<stage> -DinstanceUri=<HostAndPort>


License and Trademarks
======================

Copyright © 2015 Cask Data, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the
License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
either express or implied. See the License for the specific language governing permissions
and limitations under the License.

Cask is a trademark of Cask Data, Inc. All rights reserved.
