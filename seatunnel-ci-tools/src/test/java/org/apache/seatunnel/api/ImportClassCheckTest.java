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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.github.javaparser.JavaParser;
import com.github.javaparser.ParseResult;
import com.github.javaparser.ast.CompilationUnit;
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

    private static Map<String, List<ImportClassEntry>> importsMap = new HashMap<>();
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
                                        importsMap.put(
                                                path.toString(),
                                                extractImportEntries(result.get()));
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
    public void commonLang2Check() {
        // both common-lang and common-lang3 share the same prefix org.apache.commons.lang
        Map<String, List<String>> commonLangMap =
                checkImportClassPrefix(
                        Arrays.asList("org.apache.commons.lang"),
                        Collections.emptyList(),
                        Collections.emptyList());
        // common-lang3
        Map<String, List<String>> commonLang3Map =
                checkImportClassPrefix(
                        Arrays.asList("org.apache.commons.lang3"),
                        Collections.emptyList(),
                        Collections.emptyList());

        // find the one in common-lang but not common-lang3
        Map<String, List<String>> errorMap =
                commonLangMap.entrySet().stream()
                        .filter(entry -> !commonLang3Map.containsKey(entry.getKey()))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        Assertions.assertEquals(
                0, errorMap.size(), shadeErrorMsg("org.apache.commons.lang", errorMap));
        log.info("check org.apache.commons.lang successfully");
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
    public void hikariShadeCheck() {
        Map<String, List<String>> errorMap =
                checkImportClassPrefixWithAll(Collections.singletonList("com.zaxxer.hikari"));
        Assertions.assertEquals(0, errorMap.size(), shadeErrorMsg("hikari", errorMap));
        log.info("check hikari shade successfully");
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
    public void commonLang3Check() {
        Map<String, List<String>> errorMap =
                checkImportClassPrefixWithAll(
                        Collections.singletonList("org.apache.commons.lang3"));
        Assertions.assertEquals(0, errorMap.size(), shadeErrorMsg("commons.lang3", errorMap));
        log.info("check common lang3 shade successfully");
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

    @Test
    public void shouldKeepImportNameAndLineWhenPrecomputingImportEntries() {
        ParseResult<CompilationUnit> parseResult =
                JAVA_PARSER.parse(
                        "package org.apache.seatunnel.api;\n"
                                + "import java.util.List;\n"
                                + "import org.apache.commons.lang3.StringUtils;\n"
                                + "class Sample {}");
        CompilationUnit compilationUnit = parseResult.getResult().orElseThrow(AssertionError::new);

        List<ImportClassEntry> importEntries = extractImportEntries(compilationUnit);

        Assertions.assertEquals(2, importEntries.size());
        Assertions.assertEquals("java.util.List", importEntries.get(0).getImportClassName());
        Assertions.assertEquals(2, importEntries.get(0).getLineNumber());
        Assertions.assertEquals(
                "org.apache.commons.lang3.StringUtils", importEntries.get(1).getImportClassName());
        Assertions.assertEquals(3, importEntries.get(1).getLineNumber());
        Assertions.assertEquals(
                "org.apache.commons.lang3.StringUtils  [3]",
                formatImportClassLine(importEntries.get(1)));
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
                                                importEntry ->
                                                        prefixList.stream()
                                                                .anyMatch(
                                                                        prefix ->
                                                                                importEntry
                                                                                        .getImportClassName()
                                                                                        .startsWith(
                                                                                                prefix)))
                                        .map(this::formatImportClassLine)
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

    private static List<ImportClassEntry> extractImportEntries(CompilationUnit compilationUnit) {
        return compilationUnit.getImports().stream()
                .map(
                        importDeclaration ->
                                new ImportClassEntry(
                                        importDeclaration.getNameAsString(),
                                        importDeclaration
                                                .getRange()
                                                .map(range -> range.end.line)
                                                .orElse(-1)))
                .collect(Collectors.toList());
    }

    private String formatImportClassLine(ImportClassEntry importClassEntry) {
        return String.format(
                "%s  [%s]",
                importClassEntry.getImportClassName(), importClassEntry.getLineNumber());
    }

    @AfterAll
    public static void cleanup() {
        importsMap.clear();
    }

    private static final class ImportClassEntry {
        private final String importClassName;
        private final int lineNumber;

        private ImportClassEntry(String importClassName, int lineNumber) {
            this.importClassName = importClassName;
            this.lineNumber = lineNumber;
        }

        private String getImportClassName() {
            return importClassName;
        }

        private int getLineNumber() {
            return lineNumber;
        }
    }
}
