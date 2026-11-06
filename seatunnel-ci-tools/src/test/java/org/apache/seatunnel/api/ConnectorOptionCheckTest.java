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

package org.apache.seatunnel.api;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.github.javaparser.JavaParser;
import com.github.javaparser.ParseResult;
import com.github.javaparser.ast.CompilationUnit;
import com.github.javaparser.ast.body.ClassOrInterfaceDeclaration;
import com.github.javaparser.symbolsolver.JavaSymbolSolver;
import com.github.javaparser.symbolsolver.resolution.typesolvers.CombinedTypeSolver;
import com.github.javaparser.symbolsolver.resolution.typesolvers.JavaParserTypeSolver;
import com.github.javaparser.symbolsolver.resolution.typesolvers.ReflectionTypeSolver;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class ConnectorOptionCheckTest {

    private static final String javaPathFragment =
            "src" + File.separator + "main" + File.separator + "java";
    private static final String JAVA_FILE_EXTENSION = ".java";
    private static final String CONNECTOR_DIR = "seatunnel-connectors-v2";
    private static final Path SOURCE_ROOT_FRAGMENT = Paths.get("src", "main", "java");
    private static final String SOURCE_FQN = "org.apache.seatunnel.api.source.SeaTunnelSource";
    private static final String SINK_FQN = "org.apache.seatunnel.api.sink.SeaTunnelSink";

    /**
     * Directly known connector base classes. Only used as the fallback when the type hierarchy of a
     * class cannot be resolved; the resolved check covers these and every other transitive base
     * class automatically.
     */
    private static final Set<String> DIRECT_CONNECTOR_BASE_CLASSES =
            new HashSet<>(
                    Arrays.asList(
                            "AbstractSimpleSink",
                            "AbstractSingleSplitSource",
                            "IncrementalSource",
                            "BaseMultipleTableFileSink",
                            "BaseFileSource",
                            "BaseFileSink",
                            "HttpSource",
                            "HttpSink"));

    private static final JavaParser JAVA_PARSER;

    static {
        CombinedTypeSolver typeSolver = new CombinedTypeSolver();
        typeSolver.add(new ReflectionTypeSolver());
        try (Stream<Path> paths = Files.walk(Paths.get(".."), FileVisitOption.FOLLOW_LINKS)) {
            // Only real source roots can resolve symbols; registering nested directories or
            // individual files only slows every symbol resolution down.
            paths.filter(Files::isDirectory)
                    .filter(path -> path.endsWith(SOURCE_ROOT_FRAGMENT))
                    .forEach(
                            path -> {
                                try {
                                    typeSolver.add(new JavaParserTypeSolver(path.toFile()));
                                } catch (Exception e) {
                                    // ignore
                                }
                            });
        } catch (IOException e) {
            log.error("Failed to setup type solver", e);
        }
        JAVA_PARSER = new JavaParser();
        JAVA_PARSER.getParserConfiguration().setSymbolResolver(new JavaSymbolSolver(typeSolver));
    }

    @Test
    public void checkConnectorOptionExist() {
        // A TreeSet keeps the failure output stable across runs.
        Set<String> connectorOptionFileNames = new TreeSet<>();
        try (Stream<Path> paths = Files.walk(Paths.get(".."), FileVisitOption.FOLLOW_LINKS)) {
            List<Path> connectorClassPaths =
                    paths.filter(
                                    path -> {
                                        String pathString = path.toString();
                                        return pathString.endsWith(JAVA_FILE_EXTENSION)
                                                && pathString.contains(CONNECTOR_DIR)
                                                && pathString.contains(javaPathFragment);
                                    })
                            .collect(Collectors.toList());
            connectorClassPaths.forEach(
                    path -> {
                        try {
                            ParseResult<CompilationUnit> parseResult =
                                    JAVA_PARSER.parse(Files.newInputStream(path));
                            parseResult
                                    .getResult()
                                    .ifPresent(
                                            compilationUnit -> {
                                                List<ClassOrInterfaceDeclaration> classes =
                                                        compilationUnit.findAll(
                                                                ClassOrInterfaceDeclaration.class);
                                                for (ClassOrInterfaceDeclaration classDeclaration :
                                                        classes) {
                                                    if (classDeclaration.isAbstract()
                                                            || classDeclaration.isInterface()) {
                                                        continue;
                                                    }
                                                    if (isSeaTunnelConnector(classDeclaration)) {
                                                        connectorOptionFileNames.add(
                                                                classNameOf(path)
                                                                        .concat("Options"));
                                                    }
                                                }
                                            });
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
            connectorClassPaths.forEach(path -> connectorOptionFileNames.remove(classNameOf(path)));

            Assertions.assertEquals(
                    0,
                    connectorOptionFileNames.size(),
                    () ->
                            "Connector class does not have correspondingly [Options] class. "
                                    + "The connector need put all parameter into <ConnectorClassName>Options classes, like [ActivemqSink] and [ActivemqSinkOptions].\n"
                                    + "Those [Options] class are missing: \n"
                                    + String.join("\n", connectorOptionFileNames)
                                    + "\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String classNameOf(Path path) {
        String fileName = path.getFileName().toString();
        return fileName.endsWith(JAVA_FILE_EXTENSION)
                ? fileName.substring(0, fileName.length() - JAVA_FILE_EXTENSION.length())
                : fileName;
    }

    private boolean isSeaTunnelConnector(ClassOrInterfaceDeclaration classDeclaration) {
        try {
            // Resolve the whole hierarchy instead of matching only the directly declared
            // types: connectors that implement SeaTunnelSource/SeaTunnelSink through an
            // intermediate base class were missed by the direct check.
            return classDeclaration.resolve().getAllAncestors().stream()
                    .anyMatch(
                            ancestor -> {
                                String name = ancestor.getQualifiedName();
                                return SOURCE_FQN.equals(name) || SINK_FQN.equals(name);
                            });
        } catch (Exception e) {
            // Fall back to direct-name matching when the hierarchy cannot be resolved, for
            // example when an ancestor comes from a dependency that is not a source root.
            return matchesConnectorByDirectTypes(classDeclaration);
        }
    }

    private boolean matchesConnectorByDirectTypes(ClassOrInterfaceDeclaration classDeclaration) {
        return classDeclaration.getImplementedTypes().stream()
                        .anyMatch(
                                type -> {
                                    String name = type.getNameAsString();
                                    return name.equals("SeaTunnelSource")
                                            || name.equals("SeaTunnelSink");
                                })
                || classDeclaration.getExtendedTypes().stream()
                        .anyMatch(
                                type ->
                                        DIRECT_CONNECTOR_BASE_CLASSES.contains(
                                                type.getNameAsString()));
    }
}
