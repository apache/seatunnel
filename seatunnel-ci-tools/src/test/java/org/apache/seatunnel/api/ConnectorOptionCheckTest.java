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
import com.github.javaparser.ast.NodeList;
import com.github.javaparser.ast.body.ClassOrInterfaceDeclaration;
import com.github.javaparser.ast.type.ClassOrInterfaceType;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class ConnectorOptionCheckTest {

    private static final String javaPathFragment =
            "src" + File.separator + "main" + File.separator + "java";
    private static final String JAVA_FILE_EXTENSION = ".java";
    private static final String CONNECTOR_DIR = "seatunnel-connectors-v2";
    private static final JavaParser JAVA_PARSER = new JavaParser();

    @Test
    public void checkConnectorOptionExist() {
        Set<String> connectorOptionFileNames = new HashSet<>();
        Set<String> whiteListConnectorOptionFileNames = buildWhiteList();
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
                                                    NodeList<ClassOrInterfaceType>
                                                            implementedTypes =
                                                                    classDeclaration
                                                                            .getImplementedTypes();
                                                    implementedTypes.forEach(
                                                            implementedType -> {
                                                                if (implementedType
                                                                                .getNameAsString()
                                                                                .equals(
                                                                                        "SeaTunnelSource")
                                                                        || implementedType
                                                                                .getNameAsString()
                                                                                .equals(
                                                                                        "SeaTunnelSink")) {
                                                                    connectorOptionFileNames.add(
                                                                            path.getFileName()
                                                                                    .toString()
                                                                                    .replace(
                                                                                            JAVA_FILE_EXTENSION,
                                                                                            "")
                                                                                    .concat(
                                                                                            "Options"));
                                                                }
                                                            });
                                                    NodeList<ClassOrInterfaceType> extendedTypes =
                                                            classDeclaration.getExtendedTypes();
                                                    extendedTypes.forEach(
                                                            extendedType -> {
                                                                if (extendedType
                                                                                .getNameAsString()
                                                                                .equals(
                                                                                        "AbstractSimpleSink")
                                                                        || extendedType
                                                                                .getNameAsString()
                                                                                .equals(
                                                                                        "AbstractSingleSplitSource")
                                                                        || extendedType
                                                                                .getNameAsString()
                                                                                .equals(
                                                                                        "IncrementalSource")) {
                                                                    connectorOptionFileNames.add(
                                                                            path.getFileName()
                                                                                    .toString()
                                                                                    .replace(
                                                                                            JAVA_FILE_EXTENSION,
                                                                                            "")
                                                                                    .concat(
                                                                                            "Options"));
                                                                }
                                                            });
                                                }
                                            });
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
            connectorClassPaths.forEach(
                    path -> {
                        String className =
                                path.getFileName().toString().replace(JAVA_FILE_EXTENSION, "");
                        connectorOptionFileNames.remove(className);
                    });

            whiteListConnectorOptionFileNames.forEach(
                    whiteListConnectorOptionFileName -> {
                        Assertions.assertTrue(
                                connectorOptionFileNames.remove(whiteListConnectorOptionFileName),
                                "This [Options] class is in white list, but not found related connector classes, please check: ["
                                        + whiteListConnectorOptionFileName
                                        + "]\n");
                    });

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

    private Set<String> buildWhiteList() {
        Set<String> whiteList = new HashSet<>();
        whiteList.add("JdbcSinkOptions");
        whiteList.add("TypesenseSourceOptions");
        whiteList.add("RabbitmqSourceOptions");
        whiteList.add("TypesenseSinkOptions");
        whiteList.add("EmailSinkOptions");
        whiteList.add("HudiSinkOptions");
        whiteList.add("PulsarSinkOptions");
        whiteList.add("HttpSinkOptions");
        whiteList.add("SlsSinkOptions");
        whiteList.add("DingTalkSinkOptions");
        whiteList.add("Neo4jSinkOptions");
        whiteList.add("MaxcomputeSinkOptions");
        whiteList.add("PaimonSinkOptions");
        whiteList.add("TDengineSourceOptions");
        whiteList.add("PulsarSourceOptions");
        whiteList.add("FakeSourceOptions");
        whiteList.add("HbaseSinkOptions");
        whiteList.add("MongodbSinkOptions");
        whiteList.add("IoTDBSinkOptions");
        whiteList.add("EasysearchSourceOptions");
        whiteList.add("RabbitmqSinkOptions");
        whiteList.add("IcebergSourceOptions");
        whiteList.add("HbaseSourceOptions");
        whiteList.add("PaimonSourceOptions");
        whiteList.add("IoTDBSourceOptions");
        whiteList.add("SlsSourceOptions");
        whiteList.add("SentrySinkOptions");
        whiteList.add("EasysearchSinkOptions");
        whiteList.add("QdrantSinkOptions");
        whiteList.add("MilvusSourceOptions");
        whiteList.add("RocketMqSinkOptions");
        whiteList.add("ClickhouseFileSinkOptions");
        whiteList.add("IcebergSinkOptions");
        whiteList.add("MaxcomputeSourceOptions");
        whiteList.add("InfluxDBSourceOptions");
        whiteList.add("InfluxDBSinkOptions");
        whiteList.add("KuduSourceOptions");
        whiteList.add("SocketSinkOptions");
        whiteList.add("DataHubSinkOptions");
        whiteList.add("ClickhouseSinkOptions");
        whiteList.add("SelectDBSinkOptions");
        whiteList.add("ConsoleSinkOptions");
        whiteList.add("PrometheusSinkOptions");
        whiteList.add("FirestoreSinkOptions");
        whiteList.add("ClickhouseSourceOptions");
        whiteList.add("MilvusSinkOptions");
        whiteList.add("RocketMqSourceOptions");
        whiteList.add("TablestoreSinkOptions");
        whiteList.add("TableStoreDBSourceOptions");
        whiteList.add("KuduSinkOptions");
        whiteList.add("TDengineSinkOptions");
        whiteList.add("Neo4jSourceOptions");
        whiteList.add("HttpSourceOptions");
        whiteList.add("QdrantSourceOptions");
        whiteList.add("SheetsSourceOptions");
        whiteList.add("SocketSourceOptions");
        whiteList.add("OpenMldbSourceOptions");
        whiteList.add("Web3jSourceOptions");
        whiteList.add("PostgresIncrementalSourceOptions");
        whiteList.add("SqlServerIncrementalSourceOptions");
        whiteList.add("OracleIncrementalSourceOptions");
        whiteList.add("MySqlIncrementalSourceOptions");
        whiteList.add("MongodbIncrementalSourceOptions");
        return whiteList;
    }
}
