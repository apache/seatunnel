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

package org.apache.seatunnel.api.connector;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MarkdownHeaderTest {

    private static final List<Path> docsDirectorys = new ArrayList<>();

    @BeforeAll
    public static void setup() {
        docsDirectorys.add(Paths.get("..", "docs", "en"));
        docsDirectorys.add(Paths.get("..", "docs", "zh"));
    }

    @Test
    public void testPrimaryHeadersHaveNoTextAbove() {
        docsDirectorys.forEach(
                docsDirectory -> {
                    try (Stream<Path> paths = Files.walk(docsDirectory)) {
                        List<Path> mdFiles =
                                paths.filter(Files::isRegularFile)
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
}
