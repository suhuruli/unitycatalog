package io.unitycatalog.spark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.Test;

/**
 * Guards against the connector depending on Spark's relocated third-party classes (for example
 * {@code org.sparkproject.guava}). Those classes are shaded into Spark only by its Maven build, so
 * a connector that references them fails with {@code NoClassDefFoundError} on an sbt-built Spark.
 */
public class NoSparkRelocatedDependenciesTest {

  private static final String RELOCATED_PREFIX = "org/sparkproject/";

  @Test
  public void connectorClassesDoNotReferenceSparkRelocatedPackages() throws Exception {
    URI location =
        UCSingleCatalog.class.getProtectionDomain().getCodeSource().getLocation().toURI();
    Path root = Paths.get(location);
    List<String> offenders;
    if (Files.isDirectory(root)) {
      offenders = findOffenders(root);
    } else {
      try (FileSystem jar = FileSystems.newFileSystem(root, (ClassLoader) null)) {
        offenders = findOffenders(jar.getPath("/"));
      }
    }
    assertThat(offenders)
        .as("connector classes referencing org.sparkproject (Spark's relocated dependencies)")
        .isEmpty();
  }

  @Test
  public void initializeWithoutUriFailsWithIllegalArgument() {
    UCSingleCatalog catalog = new UCSingleCatalog();
    assertThatThrownBy(() -> catalog.initialize("my_cat", new CaseInsensitiveStringMap(Map.of())))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("uri must be specified for Unity Catalog 'my_cat'");
  }

  @Test
  public void requireProviderSpecifiedFailsWithIllegalArgument() {
    assertThatThrownBy(
            () -> UCSingleCatalog.requireProviderSpecified("CREATE TABLE", Collections.emptyMap()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("CREATE TABLE requires USING <format> (for example, USING DELTA)");
  }

  private static List<String> findOffenders(Path root) throws IOException {
    try (Stream<Path> files = Files.walk(root)) {
      return files
          .filter(p -> p.toString().endsWith(".class"))
          .filter(NoSparkRelocatedDependenciesTest::referencesRelocatedPackage)
          .map(p -> root.relativize(p).toString())
          .sorted()
          .collect(Collectors.toList());
    }
  }

  private static boolean referencesRelocatedPackage(Path classFile) {
    try {
      // Class names in the constant pool are stored as modified UTF-8, which is ASCII-compatible.
      return new String(Files.readAllBytes(classFile), StandardCharsets.ISO_8859_1)
          .contains(RELOCATED_PREFIX);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
