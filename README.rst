CDAP Integration Tests
======================

CDAP integration tests run against an existing CDAP instance.

Writing Tests
-------------
New test cases should be written in the ``integration-test-remote`` module, alongside the existing test cases.
They can be within their own package, as long as it is a "sub-package" of ``io.cdap.cdap.apps``.

When writing an integration test, the same APIs are available as in CDAP unit tests.
Note that some APIs are not available, such as the ``TestManager`` methods ``deployTemplate``, ``addTemplatePlugins``,
and ``getDataset``.

For instance, to compile and deploy an Application class and start a Service of that application::

  ApplicationManager applicationManager = deployApplication(DataCleansingApp.class);
  applicationManager.getServiceManager("DataCleansingService").start()

If additional APIs are needed that are not available in the CDAP unit tests framework,
the CDAP clients library can be utilized to interact with the CDAP instance.

For instance, example usage of ApplicationClient is in the ApplicationTest test class. Other clients can be
instantiated in a similar manner::

  ApplicationClient appClient = new ApplicationClient(getClientConfig(), getRestClient());
  appClient.list(new NamespaceId("myns"));

If the artifacts being tested are under development, they should be loaded into the target CDAP instance prior to starting the test.

Running Tests
-------------
To run integration tests against a remote CDAP instance, execute::

  mvn clean test -DinstanceUri=<HostAndPort>
 
For local development, ``HostAndPort`` should be ``http://localhost:11015``, which is the default root url for the CDAP API.

Note that the CDAP instance against which the integration tests are run must have unrecoverable reset enabled.

Also note that if -DinstanceUri is not specified, then it will connect to the default host and port.

To run integration tests against an automatically instantiated CDAP Standalone instance, execute::

  mvn clean test -P standalone-test

To modify the timeouts for program start/stop awaiting, set the ``programTimeout`` argument to a number (in seconds).
For instance, to set the timeout to 120 seconds::

  mvn clean test -DinstanceUri=<HostAndPort> -DprogramTimeout=120

To configure the namespace used for the tests, use::

  -Dtest.namespace=<TestNamespace>

To run against a secure cluster with basic authentication, use::

  -Dcdap.username=<username> -Dcdap.password=<password>
  
Note the entire integration test takes a long time to run, to run specific tests, use::

  -Dtest=<TestName>
  
If the test has dependencies on GCP components, add the following property::

  -Dgoogle.application.credentials.path=<PathToCredentialFile>

Troubleshooting Tests
-------------

If the test fails, calling ``http://localhost:11015/v3/namespaces/<namespace>/apps/<app-id>/versions/<version>/workflows/DataPipelineWorkflow/runs`` will return the run id, use this to see the application logs at ``<HostAndPort>>/v3/namespaces/<namespace>/apps/<app-id>>/workflows/DataPipelineWorkflow/runs/<run-id>>/logs``.

See here_ for API reference.

.. _here: https://docs.cask.co/cdap/6.0.0/en/reference-manual/http-restful-api/logging.html

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


CDAP Long Running Tests
------------------
The long running test cases consist of 4 operations, ``setup``, ``cleanup``, ``verifyRuns`` and ``runOperations``.
More information on design of long running tests can be found [here](https://confluence.cask.co/display/ENG/Long+Running+Tests)

All the long running tests/test-packages should be under ``<cdap-integration-tests>/long-running-test/test/java/co/cask/cdap/longrunning``

To run all the tests under ``longrunning`` package::

  mvn clean test -P long-running-test -DinstanceUri=<cdap-host>:<cdap-port> -Dinput.state=./long-running-test-in.state -Doutput.state=./long-running-test-out.state

Here, ``-Dinput.state`` is an input file from previous run which will be used in current run.
``-Doutput.state`` is an output file which will be used to persist state of current run.

To run selected single/multiple tests under longrunning package::

  mvn clean test -P long-running-test -DinstanceUri=<cdap-host>:<cdap-port> -Dinput.state=./long-running-test-in.state -Doutput.state=./long-running-test-out.state -Dlong.test=IncrementTest,DataCleansingTest -Dlong.running.namespace=testNamespace

Here, ``-Dlong.test`` is used to specify multiple comma separated tests.
``-Dlong.running.namespace`` is used to specify namespace name for all long running tests. If not specified, 'Default' namespace will be used.

CDAP perf-tests
------------------

For running generic database plugin test:
```
mvn clean test -P perf-tests -Dtest=DBBaseTest -Ddatabase.connectionString="jdbc:mysql://localhost:3306/demo" \
-Ddatabase.importQuery="SELECT * FROM my_table WHERE $CONDITIONS" \
-Ddatabase.driverName=mysql -Ddatabase.user=root -Ddatabase.password=mysql \
-Ddatabase.sinkTable=sink_table \
```

For running specific database plugin test:
```
mvn clean test -P perf-tests -Dtest=DBSpecificTest -Ddatabase.host="localhost" -Ddatabase.port=35021 \
-Ddatabase.database=EE -Ddatabase.importQuery="SELECT * FROM my_table WHERE $CONDITIONS" -Ddatabase.driverName=oracle \
-Ddatabase.user=ora -Ddatabase.password=cdap -Ddatabase.sinkTable=sink_table -Ddatabase.pluginName=Oracle
```

Chaos Monkey in ITN
------------------
Tests that extend DisruptionTestBase can use getClusterDisruptor() to get an instance of ChaosMonkeyService. ContinuousCounterTest can be used as a reference for testing with chaos monkey.

To locally run tests with DisruptionTestBase::

  mvn clean test -DinstanceUri=<HostAndPort> -Dcoopr.cluster.id=<CooprClusterID>

Additional properties include::

  ssh.username - ssh username, if it different from system user
  ssh.passphrase - private key passphrase, if applicable
  ssh.private.key - path to the private key, will check common key spots like ~/.ssh/id_rsa if not provided

To run a test using DisruptionTestBase against a non-Coopr cluster, refer to https://github.com/cdapio/chaos-monkey for steps in cluster information collector setup

To run a disruption test on bamboo, add the test to DisruptionTests in DisruptionTestSuite and manually launch a run from Integration Tests - Disruption

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
