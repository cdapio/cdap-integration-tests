/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.app.etl;

import co.cask.cdap.api.artifact.ArtifactRange;
import co.cask.cdap.api.artifact.ArtifactVersion;
import co.cask.cdap.app.serviceworker.ServiceWorkerTest;
import co.cask.cdap.client.ArtifactClient;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.common.ContentProvider;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Set;

public class UDDTest extends ETLTestBase {

  private static final ArtifactId artifactId = TEST_NAMESPACE.artifact("image-app", "1.0.0");

  @Test
  public void test() throws Exception {
    // Now testing the artifact listing / class loading using the Artifact HTTP Service
    final File directiveJar =
      new File(ServiceWorkerTest.class.getClassLoader().getResource("simple-udds-1.0-SNAPSHOT.jar").toURI());

    ArtifactClient artifactClient = new ArtifactClient(getClientConfig(), getRestClient());

    Set<ArtifactRange> parentArtifacts = new HashSet<>();
    parentArtifacts.add(new ArtifactRange(NamespaceId.SYSTEM.getNamespace(),
                                          "wrangler-transform",
                                          new ArtifactVersion("3.0.0"),
                                          new ArtifactVersion("10.0.0-SNAPSHOT")));

    artifactClient.add(artifactId, parentArtifacts, new ContentProvider<InputStream>() {
      @Override
      public InputStream getInput() throws IOException {
        return new FileInputStream(directiveJar);
      }
    });
    int i = 2;
  }
}
