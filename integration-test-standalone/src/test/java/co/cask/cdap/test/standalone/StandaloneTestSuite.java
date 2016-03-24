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

package co.cask.cdap.test.standalone;

import co.cask.cdap.StandaloneTester;
import co.cask.cdap.common.lang.ClassLoaders;
import co.cask.cdap.test.runner.AutoSuiteRunner;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.ExternalResource;
import org.junit.runner.RunWith;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
@RunWith(AutoSuiteRunner.class)
@AutoSuiteRunner.Matches(packages = "co.cask.cdap.apps")
public class StandaloneTestSuite {

  private static final Object STANDALONE = createStandaloneTester();

  @ClassRule
  public static final ExternalResource STANDALONE_RESOURCE = new ExternalResource() {

    // Since the STANDALONE instance is created from a different ClassLoader,
    // we need to trigger call to before and after using reflection.

    @Override
    protected void before() throws Throwable {
      ClassLoader oldClassLoader = ClassLoaders.setContextClassLoader(STANDALONE.getClass().getClassLoader());
      try {
        Method before = STANDALONE.getClass().getDeclaredMethod("before");
        before.setAccessible(true);
        before.invoke(STANDALONE);
      } finally {
        ClassLoaders.setContextClassLoader(oldClassLoader);
      }
    }

    @Override
    protected void after() {
      ClassLoader oldClassLoader = ClassLoaders.setContextClassLoader(STANDALONE.getClass().getClassLoader());
      try {
        Method after = STANDALONE.getClass().getDeclaredMethod("after");
        after.setAccessible(true);
        after.invoke(STANDALONE);
      } catch (Exception e) {
        throw Throwables.propagate(e);
      } finally {
        ClassLoaders.setContextClassLoader(oldClassLoader);
      }
    }
  };

  @BeforeClass
  public static void init() throws Exception {
    // Need to use reflection to call method on the STANDALONE object since it is loaded from a different ClassLoader
    System.setProperty("instanceUri", STANDALONE.getClass().getMethod("getBaseURI").invoke(STANDALONE).toString());
  }

  /**
   * Creates a new instance of StandaloneTester with a new ClassLoader that excludes test application jars.
   */
  private static Object createStandaloneTester() {
    List<URL> urls = new ArrayList<>();
    for (String path : Splitter.on(':').split(System.getProperty("java.class.path"))) {

      // A small hack is needed since we didn't make example application under different groupId and not prefixed
      // the artifactId with something special (cdap- and hive-)
      // Check for co/cask/cdap
      if (path.contains("/co/cask/cdap/")) {
        String artifactFile = Paths.get(path).getFileName().toString();
        if (!artifactFile.startsWith("cdap-") && !artifactFile.startsWith("hive-")) {
          continue;
        }
      }

      try {
        urls.add(Paths.get(path).toUri().toURL());
      } catch (MalformedURLException e) {
        throw Throwables.propagate(e);
      }
    }

    URLClassLoader classLoader = new URLClassLoader(urls.toArray(new URL[urls.size()]), null);
    try {
      Class<?> clz = classLoader.loadClass(StandaloneTester.class.getName());
      Constructor<?> constructor = clz.getConstructor(Object[].class);
      return constructor.newInstance((Object) new Object[0]);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
