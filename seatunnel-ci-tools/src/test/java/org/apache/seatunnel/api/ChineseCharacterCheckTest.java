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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.github.javaparser.JavaParser;
import com.github.javaparser.ParseResult;
import com.github.javaparser.ast.CompilationUnit;
import com.github.javaparser.ast.comments.Comment;
import com.github.javaparser.ast.visitor.VoidVisitorAdapter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.apache.seatunnel.api.ImportClassCheckTest.isWindows;

@Slf4j
public class ChineseCharacterCheckTest {

    private final JavaParser JAVA_PARSER = new JavaParser();

    private static final Pattern CHINESE_PATTERN = Pattern.compile("[\\u4e00-\\u9fa5]");

    /** Defines what content should be checked for Chinese characters */
    public enum CheckScope {
        /** Check both comments and code */
        ALL,
        /** Check only comments */
        COMMENTS_ONLY,
        /** Check only code (string literals) */
        CODE_ONLY
    }

    @Disabled("Currently only checking comments")
    @Test
    public void checkChineseCharactersInAll() {
        checkChineseCharacters(CheckScope.ALL);
    }

    @Test
    public void checkChineseCharactersInCommentsOnly() {
        checkChineseCharacters(CheckScope.COMMENTS_ONLY);
    }

    @Disabled("Currently only checking comments")
    @Test
    public void checkChineseCharactersInCodeOnly() {
        checkChineseCharacters(CheckScope.CODE_ONLY);
    }

    private void checkChineseCharacters(CheckScope scope) {
        // Define path fragments for source and test Java files
        String mainPathFragment = isWindows ? "src\\main\\java" : "src/main/java";
        String testPathFragment2 = isWindows ? "src\\test\\java" : "src/test/java";

        try (Stream<Path> paths = Files.walk(Paths.get(".."), FileVisitOption.FOLLOW_LINKS)) {
            List<String> filesWithChinese = new ArrayList<>();

            // Filter Java files in the specified directories
            paths.filter(
                            path -> {
                                String pathString = path.toString();
                                return pathString.endsWith(".java")
                                        && (pathString.contains(mainPathFragment)
                                                || pathString.contains(testPathFragment2));
                            })
                    .forEach(
                            path -> {
                                try {
                                    // Parse the Java file
                                    ParseResult<CompilationUnit> parseResult =
                                            JAVA_PARSER.parse(Files.newInputStream(path));

                                    parseResult
                                            .getResult()
                                            .ifPresent(
                                                    cu -> {
                                                        // Check for Chinese characters in comments
                                                        // if needed
                                                        if (scope != CheckScope.CODE_ONLY) {
                                                            List<Comment> comments =
                                                                    cu.getAllContainedComments();
                                                            for (Comment comment : comments) {
                                                                if (CHINESE_PATTERN
                                                                        .matcher(
                                                                                comment
                                                                                        .getContent())
                                                                        .find()) {
                                                                    filesWithChinese.add(
                                                                            String.format(
                                                                                    "Found Chinese characters in comment at %s: %s",
                                                                                    path
                                                                                            .toAbsolutePath(),
                                                                                    comment.getContent()
                                                                                            .trim()));
                                                                }
                                                            }
                                                        }

                                                        // Check for Chinese characters in code if
                                                        // needed
                                                        if (scope != CheckScope.COMMENTS_ONLY) {
                                                            ChineseCharacterVisitor visitor =
                                                                    new ChineseCharacterVisitor(
                                                                            path, filesWithChinese);
                                                            visitor.visit(cu, null);
                                                        }
                                                    });

                                } catch (Exception e) {
                                    log.error("Error parsing file: {}", path, e);
                                }
                            });

            // Assert that no files contain Chinese characters
            Assertions.assertEquals(
                    0,
                    filesWithChinese.size(),
                    () ->
                            String.format(
                                    "Found Chinese characters in following files (Scope: %s):\n%s",
                                    scope, String.join("\n", filesWithChinese)));

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static class ChineseCharacterVisitor extends VoidVisitorAdapter<Void> {
        private final Path filePath;
        private final List<String> filesWithChinese;

        public ChineseCharacterVisitor(Path filePath, List<String> filesWithChinese) {
            this.filePath = filePath;
            this.filesWithChinese = filesWithChinese;
        }

        @Override
        public void visit(CompilationUnit cu, Void arg) {
            // Check for Chinese characters in string literals
            cu.findAll(com.github.javaparser.ast.expr.StringLiteralExpr.class)
                    .forEach(
                            str -> {
                                if (CHINESE_PATTERN.matcher(str.getValue()).find()) {
                                    filesWithChinese.add(
                                            String.format(
                                                    "Found Chinese characters in string literal at %s: %s",
                                                    filePath.toAbsolutePath(), str.getValue()));
                                }
                            });
            super.visit(cu, arg);
        }
    }
}
