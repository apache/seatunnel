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
import com.github.javaparser.ast.CompilationUnit;
import com.github.javaparser.ast.expr.MethodCallExpr;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.nio.file.StandardOpenOption.READ;

/**
 * Pins the case-conversion locale contract for {@code seatunnel-transforms-v2}, as agreed on issue
 * #12620.
 *
 * <p>The contract is: no case conversion in that module's production sources may depend on the
 * worker's default locale. {@code "id".toUpperCase()} returns {@code "İD"} under a Turkish or
 * Azerbaijani default, because those locales map {@code i} to {@code U+0130}, so a keyword
 * comparison that works on one worker silently stops matching on another. Every call must therefore
 * name its locale, as {@code toUpperCase(Locale.ROOT)}.
 *
 * <p>The carve-out that would normally be argued for here, "preserve the casing the user asked
 * for", is deliberately empty rather than merely unpopulated. SQL {@code LOWER} and {@code UPPER}
 * at {@code StringFunction} are the one place that could have claimed it, because they return row
 * values rather than message text. #12495 settled that by pinning both to {@code Locale.ROOT}. The
 * carve-out is therefore empty by decision, which is a stronger basis for this guard than "no
 * example exists", and it is the point a future reader is most likely to challenge.
 *
 * <p>Date and time formatting is explicitly out of scope. {@code DateTimeFormatter.ofPattern} sites
 * are behavioural work tracked separately on #12975 and #12976, and {@code ZetaDateTimeFormat}'s
 * numeric-only patterns carry no locale-sensitive text at all.
 *
 * <p>This check parses each file rather than matching its text. A regular expression has to be
 * written carefully to survive a newline between the parentheses, and it cannot tell code from a
 * comment that merely mentions the method. The abstract syntax tree has neither problem: a bare
 * call is exactly a {@link MethodCallExpr} whose argument list is empty.
 */
@Slf4j
public class TransformsCaseConversionLocaleCheckTest {

    private static final Path TRANSFORMS_V2_MAIN =
            Paths.get("..", "seatunnel-transforms-v2", "src", "main", "java");

    private static final Set<String> CASE_CONVERSION_METHODS =
            new HashSet<>(Arrays.asList("toUpperCase", "toLowerCase"));

    /**
     * The single deliberate exception to the contract. Calcite asks {@link
     * org.apache.calcite.DataContext} for the environment locale and serves it back as {@code
     * DataContext.Variable.LOCALE}, so that value has to describe the environment and cannot be
     * pinned by us. It is not a case conversion of our own. The call site carries the same
     * explanation so a reader meets it there rather than having to find this test.
     */
    private static final String LOCALE_GET_DEFAULT_EXCEPTION = "SeaTunnelDataContext.java";

    /**
     * A floor, not a pin. The control below only has to show that the scan reaches real call sites,
     * so that a zero result means "none present" rather than "nothing was read". Asserting the
     * exact number of compliant calls instead would make every legitimate addition fail this test.
     */
    private static final int MINIMUM_EXPECTED_QUALIFIED_CALLS = 20;

    private static final List<ParsedSource> SOURCES = new ArrayList<>();

    @BeforeAll
    public static void parseTransformsV2MainSources() throws IOException {
        Assertions.assertTrue(
                Files.isDirectory(TRANSFORMS_V2_MAIN),
                "Expected to find the transforms-v2 production sources at "
                        + TRANSFORMS_V2_MAIN.toAbsolutePath()
                        + ". This test resolves that path relative to the seatunnel-ci-tools module,"
                        + " so it has to be run from that module as the other checks here are.");

        JavaParser parser = new JavaParser();
        try (Stream<Path> paths = Files.walk(TRANSFORMS_V2_MAIN, FileVisitOption.FOLLOW_LINKS)) {
            List<Path> javaFiles =
                    paths.filter(Files::isRegularFile)
                            .filter(path -> path.toString().endsWith(".java"))
                            .collect(Collectors.toList());
            for (Path path : javaFiles) {
                try (InputStream in = Files.newInputStream(path, READ)) {
                    ParseResult<CompilationUnit> result = parser.parse(in);
                    CompilationUnit unit =
                            result.getResult()
                                    .orElseThrow(
                                            () ->
                                                    new IllegalStateException(
                                                            "Failed to parse " + path));
                    SOURCES.add(new ParsedSource(path, unit));
                }
            }
        }
        log.info("Parsed {} transforms-v2 production sources", SOURCES.size());
        Assertions.assertFalse(SOURCES.isEmpty(), "Parsed no sources, so nothing was checked");
    }

    @Test
    public void caseConversionNeverUsesTheDefaultLocale() {
        List<String> violations =
                methodCalls()
                        .filter(
                                located ->
                                        CASE_CONVERSION_METHODS.contains(
                                                located.call.getNameAsString()))
                        .filter(located -> located.call.getArguments().isEmpty())
                        .map(Located::describe)
                        .collect(Collectors.toList());

        Assertions.assertEquals(
                0,
                violations.size(),
                "seatunnel-transforms-v2 production code must not convert case using the default"
                        + " locale, because a Turkish or Azerbaijani worker maps 'i' to U+0130 and"
                        + " the result stops matching. Name the locale instead, for example"
                        + " toUpperCase(Locale.ROOT). Offending call sites:\n"
                        + String.join("\n", violations));
    }

    @Test
    public void theScanReachesRealCallSites() {
        // Control for the assertion above. A broken or misdirected scan would report zero bare
        // calls for the uninteresting reason that it found no calls at all, so prove on the same
        // corpus, with the same traversal, that the compliant form is being seen.
        long qualified =
                methodCalls()
                        .filter(
                                located ->
                                        CASE_CONVERSION_METHODS.contains(
                                                located.call.getNameAsString()))
                        .filter(located -> !located.call.getArguments().isEmpty())
                        .count();

        Assertions.assertTrue(
                qualified >= MINIMUM_EXPECTED_QUALIFIED_CALLS,
                "Expected to find at least "
                        + MINIMUM_EXPECTED_QUALIFIED_CALLS
                        + " locale-qualified case conversions in seatunnel-transforms-v2, but found "
                        + qualified
                        + ". A zero result from the companion check cannot be trusted unless this"
                        + " scan is demonstrably reaching real call sites.");
    }

    @Test
    public void theOnlyDefaultLocaleLookupIsTheDocumentedCalciteException() {
        List<String> unexpected =
                methodCalls()
                        .filter(located -> "getDefault".equals(located.call.getNameAsString()))
                        .filter(
                                located ->
                                        located.call
                                                .getScope()
                                                .map(scope -> isLocaleType(scope.toString()))
                                                .orElse(false))
                        .filter(located -> !LOCALE_GET_DEFAULT_EXCEPTION.equals(located.fileName()))
                        .map(Located::describe)
                        .collect(Collectors.toList());

        Assertions.assertEquals(
                0,
                unexpected.size(),
                "Locale.getDefault() is allowed in seatunnel-transforms-v2 only at "
                        + LOCALE_GET_DEFAULT_EXCEPTION
                        + ", where Calcite asks for the environment locale and it is served back as"
                        + " DataContext.Variable.LOCALE. A new reader of the default locale needs"
                        + " the same explicit justification at its call site and an entry here."
                        + " Unexpected lookups:\n"
                        + String.join("\n", unexpected));
    }

    /**
     * Matches both the imported and the fully qualified spelling. A mutation check caught this:
     * keying on the literal scope {@code "Locale"} alone let {@code java.util.Locale.getDefault()}
     * through, which is the same lookup written differently.
     */
    private static boolean isLocaleType(String scope) {
        return "Locale".equals(scope) || scope.endsWith(".Locale");
    }

    private static Stream<Located> methodCalls() {
        return SOURCES.stream()
                .flatMap(
                        source ->
                                source.unit.findAll(MethodCallExpr.class).stream()
                                        .map(call -> new Located(source.path, call)));
    }

    private static final class ParsedSource {
        private final Path path;
        private final CompilationUnit unit;

        private ParsedSource(Path path, CompilationUnit unit) {
            this.path = path;
            this.unit = unit;
        }
    }

    private static final class Located {
        private final Path path;
        private final MethodCallExpr call;

        private Located(Path path, MethodCallExpr call) {
            this.path = path;
            this.call = call;
        }

        private String fileName() {
            return path.getFileName().toString();
        }

        private String describe() {
            return "  "
                    + path
                    + ":"
                    + call.getBegin().map(pos -> String.valueOf(pos.line)).orElse("?")
                    + "  "
                    + call;
        }
    }
}
