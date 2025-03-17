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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.github.javaparser.JavaParser;
import com.github.javaparser.ParseResult;
import com.github.javaparser.Range;
import com.github.javaparser.ast.CompilationUnit;
import com.github.javaparser.ast.ImportDeclaration;
import com.github.javaparser.ast.NodeList;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.nio.file.StandardOpenOption.READ;

@Slf4j
public class ImportClassCheckTest {

    private static Map<String, NodeList<ImportDeclaration>> importsMap = new HashMap<>();
    private final String SEATUNNEL_SHADE_PREFIX = "org.apache.seatunnel.shade.";
    public static final boolean isWindows =
            System.getProperty("os.name").toLowerCase().startsWith("win");
    private static final String JAVA_FILE_EXTENSION = ".java";
    private static final JavaParser JAVA_PARSER = new JavaParser();

    @BeforeAll
    public static void beforeAll() {
        try (Stream<Path> paths = Files.walk(Paths.get(".."), FileVisitOption.FOLLOW_LINKS)) {
            paths.filter(path -> path.toString().endsWith(JAVA_FILE_EXTENSION))
                    .forEach(
                            path -> {
                                try (InputStream inputStream = Files.newInputStream(path, READ)) {
                                    ParseResult<CompilationUnit> parseResult =
                                            JAVA_PARSER.parse(inputStream);
                                    Optional<CompilationUnit> result = parseResult.getResult();
                                    if (result.isPresent()) {
                                        importsMap.put(path.toString(), result.get().getImports());
                                    } else {
                                        log.error("Failed to parse Java file: " + path);
                                    }
                                } catch (IOException e) {
                                    log.error(
                                            "IOException occurred while processing file: " + path,
                                            e);
                                }
                            });
        } catch (IOException e) {
            throw new RuntimeException("Failed to walk through directory", e);
        }
    }

    @Test
    public void guavaShadeCheck() {
        Map<String, List<String>> errorMap =
                checkImportClassPrefixWithAll(Collections.singletonList("com.google.common"));
        Assertions.assertEquals(0, errorMap.size(), shadeErrorMsg("guava", errorMap));
        log.info("check guava shade successfully");
    }

    @Test
    public void jacksonShadeCheck() {
        Map<String, List<String>> errorMap =
                checkImportClassPrefixWithExclude(
                        Collections.singletonList("com.fasterxml.jackson"),
                        Arrays.asList(
                                "org.apache.seatunnel.format.compatible.debezium.json",
                                "org.apache.seatunnel.format.compatible.kafka.connect.json",
                                "org.apache.seatunnel.connectors.druid.sink",
                                "org.apache.seatunnel.connectors.seatunnel.typesense.client"));
        Assertions.assertEquals(0, errorMap.size(), shadeErrorMsg("jackson", errorMap));
        log.info("check jackson shade successfully");
    }

    @Test
    public void jettyShadeCheck() {
        Map<String, List<String>> errorMap =
                checkImportClassPrefixWithAll(Collections.singletonList("org.eclipse.jetty"));
        Assertions.assertEquals(0, errorMap.size(), shadeErrorMsg("jetty", errorMap));
        log.info("check jetty shade successfully");
    }

    @Test
    public void janinoShadeCheck() {
        Map<String, List<String>> errorMap =
                checkImportClassPrefixWithAll(
                        Arrays.asList("org.codehaus.janino", "org.codehaus.commons"));
        Assertions.assertEquals(0, errorMap.size(), shadeErrorMsg("janino", errorMap));
        log.info("check janino shade successfully");
    }

    @Test
    public void javaUtilCompletableFutureCheck() {
        Map<String, List<String>> errorMap =
                checkImportClassPrefix(
                        Collections.singletonList("java.util.concurrent.CompletableFuture"),
                        Collections.singletonList("org.apache.seatunnel.engine"),
                        Collections.singletonList("org.apache.seatunnel.engine.e2e"));
        Assertions.assertEquals(
                0,
                errorMap.size(),
                errorMsg(
                        "Can not use java.util.concurrent.CompletableFuture, please use org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture instead.",
                        errorMap));
        log.info("check java concurrent CompletableFuture successfully");
    }

    private Map<String, List<String>> checkImportClassPrefixWithAll(List<String> prefixList) {
        return checkImportClassPrefix(prefixList, Collections.emptyList(), Collections.emptyList());
    }

    private Map<String, List<String>> checkImportClassPrefixWithExclude(
            List<String> prefixList, List<String> packageWhiteList) {
        return checkImportClassPrefix(prefixList, Collections.emptyList(), packageWhiteList);
    }

    private Map<String, List<String>> checkImportClassPrefixWithInclude(
            List<String> prefixList, List<String> packageCheckList) {
        return checkImportClassPrefix(prefixList, packageCheckList, Collections.emptyList());
    }

    private Map<String, List<String>> checkImportClassPrefix(
            List<String> prefixList, List<String> packageCheckList, List<String> packageWhiteList) {
        List<String> pathWhiteList =
                packageWhiteList.stream()
                        .map(whitePackage -> whitePackage.replace(".", isWindows ? "\\" : "/"))
                        .collect(Collectors.toList());
        List<String> pathCheckList =
                packageCheckList.stream()
                        .map(whitePackage -> whitePackage.replace(".", isWindows ? "\\" : "/"))
                        .collect(Collectors.toList());
        Map<String, List<String>> errorMap = new HashMap<>();
        importsMap.forEach(
                (clazzPath, imports) -> {
                    boolean match;
                    if (pathCheckList.isEmpty()) {
                        match = pathWhiteList.stream().noneMatch(clazzPath::contains);
                    } else {
                        match =
                                pathCheckList.stream().anyMatch(clazzPath::contains)
                                        && pathWhiteList.stream().noneMatch(clazzPath::contains);
                    }

                    if (match) {
                        List<String> collect =
                                imports.stream()
                                        .filter(
                                                importDeclaration -> {
                                                    String importClz =
                                                            importDeclaration.getName().asString();
                                                    return prefixList.stream()
                                                            .anyMatch(importClz::startsWith);
                                                })
                                        .map(this::getImportClassLineNum)
                                        .collect(Collectors.toList());
                        if (!collect.isEmpty()) {
                            errorMap.put(clazzPath, collect);
                        }
                    }
                });
        return errorMap;
    }

    private String shadeErrorMsg(String checkType, Map<String, List<String>> errorMap) {
        String msg =
                String.format("%s shade is not up to code, need add prefix [", checkType)
                        + SEATUNNEL_SHADE_PREFIX
                        + "]. \n";
        return errorMsg(msg, errorMap);
    }

    private String errorMsg(String message, Map<String, List<String>> errorMap) {
        StringBuilder msg = new StringBuilder();
        msg.append(message).append("\n");
        errorMap.forEach(
                (key, value) -> {
                    msg.append(key).append("\n");
                    value.forEach(lineNum -> msg.append(lineNum).append("\n"));
                });
        return msg.toString();
    }

    private String getImportClassLineNum(ImportDeclaration importDeclaration) {
        Range range = importDeclaration.getRange().get();
        return String.format("%s  [%s]", importDeclaration.getName().asString(), range.end.line);
    }
}
