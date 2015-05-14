CDAP Integration Tests
======================
To run integration tests, execute:

`mvn test -DargLine="-DinstanceUri=<HostAndPort>"`

Integration tests are capable of running against an existing CDAP instance or against a Standalone instance if no
instance URI is provided.


TODO
----

The directory structure of this project is not well defined.
Currently, cdap-examples is not pushed to mvn, and so the application sources are copied to this project.
Additionally, the test framework API is not finalized - several things are missing such as v3 Clients, RuntimeStats
for integration tests, etc.

License
-------

Copyright © 2014 Cask Data, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may
not use this file except in compliance with the License. You may obtain
a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
