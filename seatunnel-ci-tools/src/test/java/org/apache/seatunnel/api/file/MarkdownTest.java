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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MarkdownTest {

    private static final List<Path> docsDirectories = new ArrayList<>();

    private static final List<Path> connectorsDirectories = new ArrayList<>();

    @BeforeAll
    public static void setup() {
        docsDirectories.add(Paths.get("..", "docs", "en"));
        docsDirectories.add(Paths.get("..", "docs", "zh"));
        connectorsDirectories.add(Paths.get("..", "docs", "en", "connector-v2", "source"));
        connectorsDirectories.add(Paths.get("..", "docs", "en", "connector-v2", "sink"));
        connectorsDirectories.add(Paths.get("..", "docs", "zh", "connector-v2", "source"));
        connectorsDirectories.add(Paths.get("..", "docs", "zh", "connector-v2", "sink"));
    }

    @Test
    @DisabledOnOs(OS.WINDOWS)
    public void testChineseDocFileNameContainsInEnglishVersionDoc() {
        // Verify that the file names in the English and Chinese directories are the same.
        List<String> enFileName =
                fileName(docsDirectories.get(0)).stream()
                        .map(path -> path.replace("/en/", "/"))
                        .collect(Collectors.toList());
        List<String> zhFileName =
                fileName(docsDirectories.get(1)).stream()
                        .map(path -> path.replace("/zh/", "/"))
                        .collect(Collectors.toList());

        // Find Chinese files that don't have English counterparts
        List<String> missingEnglishFiles =
                zhFileName.stream()
                        .filter(zhFile -> !enFileName.contains(zhFile))
                        .collect(Collectors.toList());

        // If there are files missing English versions, throw an exception
        if (!missingEnglishFiles.isEmpty()) {
            StringBuilder errorMessage = new StringBuilder();
            errorMessage.append(
                    String.format(
                            "Found %d Chinese files without English versions:\n",
                            missingEnglishFiles.size()));

            missingEnglishFiles.forEach(
                    file ->
                            errorMessage.append(
                                    String.format("Missing English version for: %s\n", file)));

            throw new AssertionError(errorMessage.toString());
        }
    }

    private List<String> fileName(Path docDirectory) {
        try (Stream<Path> paths = Files.walk(docDirectory)) {
            return paths.filter(Files::isRegularFile)
                    .filter(path -> path.toString().endsWith(".md"))
                    .map(Path::toString)
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testPrimaryHeadersHaveNoTextAbove() {
        docsDirectories.forEach(
                docsDirectory -> {
                    try (Stream<Path> paths = Files.walk(docsDirectory)) {
                        List<Path> mdFiles =
                                paths.filter(Files::isRegularFile)
                                        .filter(path -> !path.getParent().endsWith("changelog"))
                                        .filter(path -> path.toString().endsWith(".md"))
                                        .collect(Collectors.toList());

                        for (Path mdPath : mdFiles) {
                            List<String> lines = Files.readAllLines(mdPath, StandardCharsets.UTF_8);

                            String firstRelevantLine = null;
                            int lineNumber = 0;
                            boolean inFrontMatter = false;

                            for (int i = 0; i < lines.size(); i++) {
                                String line = lines.get(i).trim();
                                lineNumber = i + 1;

                                if (i == 0 && line.equals("---")) {
                                    inFrontMatter = true;
                                    continue;
                                }
                                if (inFrontMatter) {
                                    if (line.equals("---")) {
                                        inFrontMatter = false;
                                    }
                                    continue;
                                }

                                if (line.isEmpty()) {
                                    continue;
                                }

                                if (line.startsWith("import ")) {
                                    continue;
                                }

                                firstRelevantLine = line;
                                break;
                            }

                            if (firstRelevantLine == null) {
                                Assertions.fail(
                                        String.format(
                                                "The file %s is empty and has no content.",
                                                mdPath));
                            }

                            if (!firstRelevantLine.startsWith("# ")) {
                                Assertions.fail(
                                        String.format(
                                                "The first line of the file %s is not a first level heading. First line content: “%s” (line number: %d)",
                                                mdPath, firstRelevantLine, lineNumber));
                            }
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    @Test
    public void testConnectorDocWithChangeLogFlagAndFile() {
        Pattern importPattern =
                Pattern.compile("import ChangeLog from '../changelog/(connector-.*).md';");
        connectorsDirectories.forEach(
                docsDirectory -> {
                    try (Stream<Path> paths = Files.walk(docsDirectory)) {
                        List<Path> mdFiles =
                                paths.filter(Files::isRegularFile)
                                        .filter(path -> path.toString().endsWith(".md"))
                                        .collect(Collectors.toList());

                        for (Path mdPath : mdFiles) {
                            List<String> lines = Files.readAllLines(mdPath, StandardCharsets.UTF_8);
                            String line = lines.get(0);
                            Assertions.assertTrue(
                                    line.startsWith("import ChangeLog from '../changelog/"),
                                    "The first line of the file "
                                            + mdPath
                                            + " is not a change log import.");
                            Matcher matcher = importPattern.matcher(line);
                            Assertions.assertTrue(
                                    matcher.matches(),
                                    "The first line of the file "
                                            + mdPath
                                            + " is not a change log import.");
                            String connector = matcher.group(1);
                            if (docsDirectory.getParent().getParent().endsWith("en")) {
                                Assertions.assertTrue(
                                        Files.exists(
                                                Paths.get(
                                                        "..",
                                                        "docs",
                                                        "en",
                                                        "connector-v2",
                                                        "changelog",
                                                        connector + ".md")),
                                        "The change log file for "
                                                + connector
                                                + " does not exist, please check "
                                                + mdPath);
                            } else {
                                Assertions.assertTrue(
                                        Files.exists(
                                                Paths.get(
                                                        "..",
                                                        "docs",
                                                        "zh",
                                                        "connector-v2",
                                                        "changelog",
                                                        connector + ".md")),
                                        "The change log file for "
                                                + connector
                                                + " does not exist, please check "
                                                + mdPath);
                            }
                            String file = String.join("\n", lines);
                            Assertions.assertTrue(
                                    file.trim().endsWith("<ChangeLog />"),
                                    "The file " + mdPath + " does not end with <ChangeLog />.");
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
    }
}
