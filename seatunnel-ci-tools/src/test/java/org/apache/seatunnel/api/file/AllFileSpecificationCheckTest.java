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

package org.apache.seatunnel.api.file;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

@Slf4j
@DisabledOnOs(OS.WINDOWS)
public class AllFileSpecificationCheckTest {

    private static Map<String, List<String>> fileContents;

    @BeforeAll
    public static void beforeAll() throws IOException {
        List<String> fileTypesCanNotRead =
                Arrays.asList("parquet", "orc", "xlsx", "xls", "png", "jar", "lzo", "zip", "ico");
        List<String> fileCanNotRead =
                Arrays.asList(
                        "seatunnel-connectors-v2/connector-file/connector-file-base/src/test/resources/encoding/gbk.json",
                        "seatunnel-connectors-v2/connector-file/connector-file-base/src/test/resources/encoding/gbk.xml",
                        "seatunnel-connectors-v2/connector-file/connector-file-base/src/test/resources/encoding/gbk_use_attr_format.xml",
                        "seatunnel-connectors-v2/connector-file/connector-file-base/src/test/resources/encoding/gbk.txt",
                        "seatunnel-e2e/seatunnel-connector-v2-e2e/connector-file-local-e2e/src/test/resources/json/e2e_gbk.json",
                        "seatunnel-e2e/seatunnel-connector-v2-e2e/connector-file-local-e2e/src/test/resources/text/e2e_gbk.txt");

        fileContents = new LinkedHashMap<>();
        try (Stream<Path> paths = Files.walk(Paths.get(".."), FileVisitOption.FOLLOW_LINKS)) {
            paths.filter(path -> path.toFile().isFile())
                    .filter(path -> !path.toFile().getName().startsWith("."))
                    .filter(
                            path ->
                                    !fileTypesCanNotRead.contains(
                                            path.toFile()
                                                    .getName()
                                                    .substring(
                                                            path.toFile().getName().lastIndexOf(".")
                                                                    + 1)))
                    .filter(path -> !fileCanNotRead.contains(path.toString().substring(3)))
                    .filter(
                            path ->
                                    !path.toString()
                                            .contains(File.separator + "target" + File.separator))
                    .filter(
                            path ->
                                    !path.toString()
                                            .contains(
                                                    File.separator
                                                            + "node_modules"
                                                            + File.separator))
                    .filter(
                            path ->
                                    !path.toString()
                                            .contains(File.separator + "node" + File.separator))
                    .filter(path -> !path.toString().contains(File.separator + "."))
                    .forEach(
                            path -> {
                                try {
                                    fileContents.put(
                                            path.toString().substring(3),
                                            Files.readAllLines(path, StandardCharsets.UTF_8));
                                } catch (IOException e) {
                                    log.error("Failed to read file: {}", path, e);
                                    throw new RuntimeException(e);
                                }
                            });
        }
    }

    @Test
    public void testFileNotContainsSourceTableNameAndResultTableName() {
        List<String> whiteList =
                Arrays.asList(
                        "seatunnel-dist/src/test/java/org/apache/seatunnel/api/file/AllFileSpecificationCheckTest.java",
                        "docs/zh/connector-v2/source-common-options.md",
                        "docs/zh/connector-v2/sink-common-options.md",
                        "docs/zh/transform-v2/common-options.md",
                        "docs/zh/concept/config.md",
                        "docs/en/connector-v2/source-common-options.md",
                        "docs/en/connector-v2/sink-common-options.md",
                        "docs/en/transform-v2/common-options.md",
                        "docs/en/concept/config.md",
                        "seatunnel-api/src/main/java/org/apache/seatunnel/api/options/ConnectorCommonOptions.java",
                        "seatunnel-e2e/seatunnel-connector-v2-e2e/connector-fake-e2e/src/test/resources/fake_to_assert_with_compatible_source_and_result_table_name.conf",
                        "seatunnel-e2e/seatunnel-connector-v2-e2e/connector-fake-e2e/src/test/java/org/apache/seatunnel/e2e/connector/fake/FakeIT.java",
                        "seatunnel-ci-tools/src/test/java/org/apache/seatunnel/api/file/AllFileSpecificationCheckTest.java");

        fileContents.forEach(
                (path, lines) -> {
                    if (whiteList.contains(path.trim())) {
                        return;
                    }
                    for (int i = 0; i < lines.size(); i++) {
                        String line = lines.get(i);
                        if (line.contains("source_table_name")
                                || line.contains("result_table_name")) {
                            throw new RuntimeException(
                                    String.format(
                                            "File %s Line %d [%s] contains `source_table_name` or `result_table_name`, please use `plugin_input` and `plugin_output` instead.",
                                            path, i + 1, line));
                        }
                    }
                });
    }
}
