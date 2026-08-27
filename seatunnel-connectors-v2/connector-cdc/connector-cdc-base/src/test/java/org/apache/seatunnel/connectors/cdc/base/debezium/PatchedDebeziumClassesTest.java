/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.cdc.base.debezium;

import org.apache.seatunnel.connectors.cdc.base.option.SourceOptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import io.debezium.config.Configuration;

import javax.xml.parsers.DocumentBuilderFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.jar.JarFile;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Pins the set of Debezium classes that connector-cdc-base recompiles and ships patched copies of.
 *
 * <p>Four of those classes also exist in {@code debezium-core}. connector-cdc-base is loaded into
 * the class loader of every CDC connector, and each connector now packages its own Debezium, so
 * both copies would be visible at runtime and the winner would depend on the order {@code
 * File#listFiles} returns the jars in {@code AbstractPluginDiscovery}. The shade configurations
 * drop the four stock copies from every connector jar so the patched ones stay the single
 * definition. {@code DefaultHeartbeatConnectionProvider} is SeaTunnel-only and therefore has no
 * stock {@code debezium-core} class to exclude.
 *
 * <p>The exclusion lists are maintained by hand in connector-cdc/pom.xml and
 * connector-cdc-opengauss/pom.xml. This test reads both lists so changing either the patched source
 * set or an exclusion fails before a connector can package nondeterministic class definitions.
 */
class PatchedDebeziumClassesTest {

    /**
     * Debezium classes patched under src/main/java/io/debezium. The four stock Debezium overrides
     * are a subset of this set; the SeaTunnel-only heartbeat provider is intentionally not.
     */
    private static final Set<String> EXPECTED_PATCHED_CLASSES =
            new TreeSet<>(
                    Arrays.asList(
                            "io/debezium/connector/base/ChangeEventQueue",
                            "io/debezium/heartbeat/DefaultHeartbeatConnectionProvider",
                            "io/debezium/heartbeat/HeartbeatFactory",
                            "io/debezium/relational/HistorizedRelationalDatabaseConnectorConfig",
                            "io/debezium/relational/TableId"));

    /**
     * Patched classes that are also present in stock debezium-core and therefore must be filtered
     * from each connector jar. DefaultHeartbeatConnectionProvider is omitted because SeaTunnel adds
     * it; it is not part of the stock Debezium artifact.
     */
    private static final Set<String> STOCK_DEBEZIUM_CORE_OVERRIDES =
            new TreeSet<>(
                    Arrays.asList(
                            "io/debezium/connector/base/ChangeEventQueue",
                            "io/debezium/heartbeat/HeartbeatFactory",
                            "io/debezium/relational/HistorizedRelationalDatabaseConnectorConfig",
                            "io/debezium/relational/TableId"));

    /** Exact shade patterns required for the stock Debezium classes and their nested classes. */
    private static final Set<String> EXPECTED_STOCK_DEBEZIUM_CORE_EXCLUDES =
            STOCK_DEBEZIUM_CORE_OVERRIDES.stream()
                    .flatMap(className -> Stream.of(className + ".class", className + "$*.class"))
                    .collect(Collectors.toCollection(TreeSet::new));

    /**
     * Verifies the Debezium classes this module compiles are exactly the maintained patched source
     * set, so a newly patched class cannot be added without reviewing its packaging implications.
     */
    @Test
    void compiledPatchedDebeziumClassesMatchExpectedSet() throws Exception {
        Path classesRoot =
                Paths.get(
                        SourceOptions.class
                                .getProtectionDomain()
                                .getCodeSource()
                                .getLocation()
                                .toURI());
        Path debeziumRoot = classesRoot.resolve("io").resolve("debezium");
        Assertions.assertTrue(
                Files.isDirectory(debeziumRoot),
                "Expected compiled Debezium overrides under " + debeziumRoot);

        Set<String> compiled;
        try (Stream<Path> paths = Files.walk(debeziumRoot)) {
            compiled =
                    paths.filter(Files::isRegularFile)
                            .map(path -> classesRoot.relativize(path).toString())
                            .filter(name -> name.endsWith(".class"))
                            .map(name -> name.substring(0, name.length() - ".class".length()))
                            // Nested and anonymous classes travel with their outer class.
                            .filter(name -> !name.contains("$"))
                            .map(name -> name.replace(File.separatorChar, '/'))
                            .collect(Collectors.toCollection(TreeSet::new));
        }

        Assertions.assertEquals(
                EXPECTED_PATCHED_CLASSES,
                compiled,
                "connector-cdc-base patches a different set of Debezium classes than this test"
                        + " documents. Review whether each new patched class also exists in"
                        + " debezium-core and update the CDC shade filters if it does.");
    }

    /**
     * Verifies both CDC shade configurations exclude each stock override, including nested classes.
     * This prevents a future source or POM-only edit from reintroducing classpath-order-dependent
     * selection between a patched base class and a stock connector class.
     */
    @Test
    void stockDebeziumCoreOverridesAreExcludedInEveryCdcShadeConfiguration() throws Exception {
        Path connectorCdcDirectory = findConnectorCdcDirectory();
        List<Path> pomFiles =
                Arrays.asList(
                        connectorCdcDirectory.resolve("pom.xml"),
                        connectorCdcDirectory
                                .resolve("connector-cdc-opengauss")
                                .resolve("pom.xml"));

        for (Path pomFile : pomFiles) {
            Assertions.assertEquals(
                    EXPECTED_STOCK_DEBEZIUM_CORE_EXCLUDES,
                    readDebeziumCoreExcludes(pomFile),
                    "The io.debezium:debezium-core shade filter in "
                            + pomFile
                            + " must exclude exactly the stock classes patched by"
                            + " connector-cdc-base.");
        }
    }

    /**
     * Verifies the fifth patched class is SeaTunnel-specific rather than a missing shade exclusion.
     * Reading the actual resolved debezium-core artifact protects this distinction when
     * dependencies are upgraded.
     */
    @Test
    void defaultHeartbeatConnectionProviderIsNotInStockDebeziumCore() throws Exception {
        Path debeziumCore =
                Paths.get(
                        Configuration.class
                                .getProtectionDomain()
                                .getCodeSource()
                                .getLocation()
                                .toURI());
        String heartbeatFactory = "io/debezium/heartbeat/HeartbeatFactory.class";
        String defaultProvider = "io/debezium/heartbeat/DefaultHeartbeatConnectionProvider.class";

        Assertions.assertTrue(
                containsClass(debeziumCore, heartbeatFactory),
                "The Configuration code source must be the resolved debezium-core artifact.");
        Assertions.assertFalse(
                containsClass(debeziumCore, defaultProvider),
                "DefaultHeartbeatConnectionProvider is SeaTunnel-only and must not be added to"
                        + " the debezium-core shade filter.");
    }

    /**
     * Locates the connector-cdc source directory from Maven's module working directory so the test
     * can inspect the two source POMs on every supported operating system.
     */
    private static Path findConnectorCdcDirectory() {
        Path current = Paths.get(System.getProperty("user.dir")).toAbsolutePath().normalize();
        while (current != null) {
            if (isConnectorCdcDirectory(current)) {
                return current;
            }

            Path repositoryCdcDirectory =
                    current.resolve("seatunnel-connectors-v2").resolve("connector-cdc");
            if (isConnectorCdcDirectory(repositoryCdcDirectory)) {
                return repositoryCdcDirectory;
            }
            current = current.getParent();
        }

        throw new IllegalStateException(
                "Cannot locate the connector-cdc source directory from Maven working directory: "
                        + System.getProperty("user.dir"));
    }

    /**
     * Identifies the connector-cdc source directory by the two POMs whose filter relationship is
     * under test, without depending on an absolute checkout path.
     */
    private static boolean isConnectorCdcDirectory(Path directory) {
        return Files.isRegularFile(directory.resolve("pom.xml"))
                && Files.isDirectory(directory.resolve("connector-cdc-base"))
                && Files.isDirectory(directory.resolve("connector-cdc-opengauss"));
    }

    /**
     * Reads the {@code debezium-core} filter from one connector POM. A secure XML parser is used
     * even though the POM is local source, so the test never resolves external entities.
     */
    private static Set<String> readDebeziumCoreExcludes(Path pomFile) throws Exception {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
        factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
        factory.setXIncludeAware(false);
        factory.setExpandEntityReferences(false);

        Document document = factory.newDocumentBuilder().parse(pomFile.toFile());
        NodeList filterNodes = document.getElementsByTagName("filter");
        List<Element> debeziumCoreFilters = new ArrayList<>();
        for (int index = 0; index < filterNodes.getLength(); index++) {
            Element filter = (Element) filterNodes.item(index);
            if ("io.debezium:debezium-core".equals(getDirectChildText(filter, "artifact"))) {
                debeziumCoreFilters.add(filter);
            }
        }

        Assertions.assertEquals(
                1,
                debeziumCoreFilters.size(),
                "Expected exactly one io.debezium:debezium-core shade filter in " + pomFile);
        Element filter = debeziumCoreFilters.get(0);
        NodeList excludeNodes = filter.getElementsByTagName("exclude");
        Set<String> excludes = new TreeSet<>();
        for (int index = 0; index < excludeNodes.getLength(); index++) {
            excludes.add(excludeNodes.item(index).getTextContent().trim());
        }
        return excludes;
    }

    /**
     * Reads the text of a direct XML child instead of matching identically named elements elsewhere
     * in a POM subtree.
     */
    private static String getDirectChildText(Element element, String childName) {
        NodeList children = element.getChildNodes();
        for (int index = 0; index < children.getLength(); index++) {
            Node child = children.item(index);
            if (child.getNodeType() == Node.ELEMENT_NODE && childName.equals(child.getNodeName())) {
                return child.getTextContent().trim();
            }
        }
        return null;
    }

    /**
     * Checks both jar and exploded-directory dependency code sources so the assertion remains valid
     * for Maven's normal test class path and IDE test runners.
     */
    private static boolean containsClass(Path codeSource, String classFile) throws IOException {
        if (Files.isDirectory(codeSource)) {
            return Files.isRegularFile(codeSource.resolve(classFile));
        }

        try (JarFile jarFile = new JarFile(codeSource.toFile())) {
            return jarFile.getJarEntry(classFile) != null;
        }
    }
}
