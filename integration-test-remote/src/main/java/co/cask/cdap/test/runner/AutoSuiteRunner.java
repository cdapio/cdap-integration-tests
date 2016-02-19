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

package co.cask.cdap.test.runner;

import com.google.common.base.CharMatcher;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.junit.runner.Description;
import org.junit.runner.Runner;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.ParentRunner;
import org.junit.runners.model.RunnerBuilder;

import java.io.File;
import java.io.IOException;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.net.URI;
import java.net.URL;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.regex.Pattern;

/**
 * Use this test {@link Runner} to automatically discover test classes to run.
 */
public class AutoSuiteRunner extends ParentRunner<Runner> {

  private final List<Runner> children;
  private static final String TESTS_TO_RUN = "long.test";

  /**
   * The <code>SuitePackages</code> annotation specifies the packages to discover test classes to run.
   */
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.TYPE)
  @Inherited
  public @interface Matches {
    /**
     * returns the packages to inspect
     */
    String[] packages();

    /**
     * returns the regex for matching class name. Default match class names end with Test.
     */
    String pattern() default ".*Test$";
  }

  public AutoSuiteRunner(Class<?> testClass, final RunnerBuilder runnerBuilder) throws Throwable {
    super(testClass);

    final List<Runner> runners = new ArrayList<>();
    final ClassLoader classLoader = getClass().getClassLoader();
    Matches matches = testClass.getAnnotation(Matches.class);
    if (matches == null) {
      throw new IllegalArgumentException("Missing @Matches annotation");
    }
    String testsToRun = System.getProperty(TESTS_TO_RUN);
    String patternStr = matches.pattern();
    if (testsToRun != null && !testsToRun.isEmpty()) {
      patternStr = createRegex(testsToRun);
    }
    Pattern pattern = Pattern.compile(patternStr);

    // Find all packages as specified
    for (String pkg : matches.packages()) {
      Enumeration<URL> resources = classLoader.getResources(pkg.replace('.', '/') + "/");
      while (resources.hasMoreElements()) {
        URL url = resources.nextElement();
        switch (url.getProtocol()) {
          case "file":
            addRunnerFromPath(pkg, Paths.get(url.toURI()), pattern, runnerBuilder, runners);
          break;
          case "jar":
            addRunnerFromJar(pkg, getJarPath(url), pattern, runnerBuilder, runners);
          break;
        }
      }
    }

    this.children = runners;
  }

  private String createRegex(String testsToRun) {
    String[] tests = testsToRun.split(",");
    StringBuilder regex = new StringBuilder();
    for (String test : tests) {
      regex.append(".*");
      regex.append(test);
      regex.append("$|");
    }
    regex.deleteCharAt((regex.length() - 1));
    return regex.toString();
  }

  @Override
  protected List<Runner> getChildren() {
    return children;
  }

  @Override
  protected Description describeChild(Runner child) {
    return child.getDescription();
  }

  @Override
  protected void runChild(Runner child, RunNotifier notifier) {
    child.run(notifier);
  }

  /**
   * Find test classes from the given path recursively and create test runner for them.
   */
  private void addRunnerFromPath(String pkg, Path path, final Pattern pattern,
                                 final RunnerBuilder runnerBuilder, final List<Runner> runners) throws IOException {
    final ClassLoader classLoader = getClass().getClassLoader();

    // Find the file path that representing the root of where the given package path starts.
    // E.g. pkg == "co.cask.cdap", path == "/root/co/cask/cdap", then pkgBase will be "/root"
    final Path pkgBase = path.getRoot().resolve(
      path.subpath(0, path.getNameCount() - CharMatcher.is('.').countIn(pkg) - 1));

    // Walk the package directory recursively.
    Files.walkFileTree(path, new FileVisitor<Path>() {
      @Override
      public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
        return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
        // Figure out the class name and create a runner for it
        Path relative = pkgBase.relativize(file);
        String className = relative.toString().replace(File.separatorChar, '.');
        className = className.substring(0, className.length() - ".class".length());

        if (!pattern.matcher(className).matches()) {
          return FileVisitResult.CONTINUE;
        }

        try {
          runners.add(runnerBuilder.runnerForClass(classLoader.loadClass(className)));
        } catch (Throwable e) {
          throw Throwables.propagate(e);
        }
        return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
        return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
        return FileVisitResult.CONTINUE;
      }
    });
  }

  /**
   * Find test classes from the given path recursively and create test runner for them.
   */
  private void addRunnerFromJar(String pkg, Path jarPath, final Pattern pattern,
                                RunnerBuilder runnerBuilder, List<Runner> runners) throws Throwable {
    ClassLoader classLoader = getClass().getClassLoader();
    // Visit every entries in the JAR and look for class files that matches the given package and pattern
    try (JarFile jarFile = new JarFile(jarPath.toFile())) {
      Enumeration<JarEntry> entries = jarFile.entries();
      String pkgPath = pkg.replace('.', '/') + '/';
      while (entries.hasMoreElements()) {
        JarEntry jarEntry = entries.nextElement();
        if (!jarEntry.getName().startsWith(pkgPath)) {
          continue;
        }
        String className = jarEntry.getName().replace('/', '.');
        className = className.substring(0, className.length() - ".class".length());
        if (!pattern.matcher(className).matches()) {
          continue;
        }
        runners.add(runnerBuilder.runnerForClass(classLoader.loadClass(className)));
      }
    }
  }

  /**
   * Returns the file path of the JAR represented by the given URL that points to an entry inside the JAR.
   */
  private Path getJarPath(URL url) {
    Preconditions.checkArgument("jar".equals(url.getProtocol()));
    // Jar entry URL has format of "jar:file/path/to/jar/file.jar!/entry/path"
    String path = url.getFile();
    return Paths.get(URI.create(path.substring(0, path.indexOf("!/"))));
  }
}
