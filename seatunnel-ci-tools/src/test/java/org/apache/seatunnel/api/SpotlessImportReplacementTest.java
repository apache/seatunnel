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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import lombok.extern.slf4j.Slf4j;

import java.util.regex.Pattern;

@Slf4j
public class SpotlessImportReplacementTest {

    // Regex patterns from pom.xml spotless configuration
    private static final String GUAVA_REGEX =
            "import\\s+(static\\s+)?com\\.google\\.common\\.([^;]+);(\\r\\n|\\r|\\n)";
    private static final String GUAVA_REPLACEMENT =
            "import $1org.apache.seatunnel.shade.com.google.common.$2;$3";

    private static final String JETTY_REGEX =
            "import\\s+(static\\s+)?org\\.eclipse\\.jetty\\.([^;]+);(\\r\\n|\\r|\\n)";
    private static final String JETTY_REPLACEMENT =
            "import $1org.apache.seatunnel.shade.org.eclipse.jetty.$2;$3";

    private static final String HIKARI_REGEX =
            "import\\s+(static\\s+)?com\\.zaxxer\\.hikari\\.([^;]+);(\\r\\n|\\r|\\n)";
    private static final String HIKARI_REPLACEMENT =
            "import $1org.apache.seatunnel.shade.com.zaxxer.hikari.$2;$3";

    private static final String JANINO_REGEX =
            "import\\s+(static\\s+)?org\\.codehaus\\.(janino|commons)\\.([^;]+);(\\r\\n|\\r|\\n)";
    private static final String JANINO_REPLACEMENT =
            "import $1org.apache.seatunnel.shade.org.codehaus.$2.$3;$4";

    @Test
    public void testGuavaImportReplacement() {
        Pattern pattern = Pattern.compile(GUAVA_REGEX);

        // Test regular import
        String input = "import com.google.common.collect.Lists;\n";
        String expected = "import org.apache.seatunnel.shade.com.google.common.collect.Lists;\n";
        String result = pattern.matcher(input).replaceAll(GUAVA_REPLACEMENT);
        Assertions.assertEquals(expected, result);

        // Test static import
        String staticInput = "import static com.google.common.base.Preconditions.checkNotNull;\n";
        String staticExpected =
                "import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkNotNull;\n";
        String staticResult = pattern.matcher(staticInput).replaceAll(GUAVA_REPLACEMENT);
        Assertions.assertEquals(staticExpected, staticResult);

        log.info("Guava import replacement test passed");
    }

    @Test
    public void testJettyImportReplacement() {
        Pattern pattern = Pattern.compile(JETTY_REGEX);

        // Test regular import
        String input = "import org.eclipse.jetty.server.Server;\n";
        String expected = "import org.apache.seatunnel.shade.org.eclipse.jetty.server.Server;\n";
        String result = pattern.matcher(input).replaceAll(JETTY_REPLACEMENT);
        Assertions.assertEquals(expected, result);

        // Test static import
        String staticInput = "import static org.eclipse.jetty.http.HttpStatus.OK_200;\n";
        String staticExpected =
                "import static org.apache.seatunnel.shade.org.eclipse.jetty.http.HttpStatus.OK_200;\n";
        String staticResult = pattern.matcher(staticInput).replaceAll(JETTY_REPLACEMENT);
        Assertions.assertEquals(staticExpected, staticResult);

        log.info("Jetty import replacement test passed");
    }

    @Test
    public void testHikariImportReplacement() {
        Pattern pattern = Pattern.compile(HIKARI_REGEX);

        // Test regular import
        String input = "import com.zaxxer.hikari.HikariDataSource;\n";
        String expected = "import org.apache.seatunnel.shade.com.zaxxer.hikari.HikariDataSource;\n";
        String result = pattern.matcher(input).replaceAll(HIKARI_REPLACEMENT);
        Assertions.assertEquals(expected, result);

        // Test static import
        String staticInput = "import static com.zaxxer.hikari.HikariConfig.MINIMUM_IDLE;\n";
        String staticExpected =
                "import static org.apache.seatunnel.shade.com.zaxxer.hikari.HikariConfig.MINIMUM_IDLE;\n";
        String staticResult = pattern.matcher(staticInput).replaceAll(HIKARI_REPLACEMENT);
        Assertions.assertEquals(staticExpected, staticResult);

        log.info("Hikari import replacement test passed");
    }

    @Test
    public void testJaninoImportReplacement() {
        Pattern pattern = Pattern.compile(JANINO_REGEX);

        // Test janino import
        String janinoInput = "import org.codehaus.janino.ExpressionEvaluator;\n";
        String janinoExpected =
                "import org.apache.seatunnel.shade.org.codehaus.janino.ExpressionEvaluator;\n";
        String janinoResult = pattern.matcher(janinoInput).replaceAll(JANINO_REPLACEMENT);
        Assertions.assertEquals(janinoExpected, janinoResult);

        // Test commons import
        String commonsInput = "import org.codehaus.commons.compiler.CompileException;\n";
        String commonsExpected =
                "import org.apache.seatunnel.shade.org.codehaus.commons.compiler.CompileException;\n";
        String commonsResult = pattern.matcher(commonsInput).replaceAll(JANINO_REPLACEMENT);
        Assertions.assertEquals(commonsExpected, commonsResult);

        // Test static janino import
        String staticInput = "import static org.codehaus.janino.Scanner.KEYWORD;\n";
        String staticExpected =
                "import static org.apache.seatunnel.shade.org.codehaus.janino.Scanner.KEYWORD;\n";
        String staticResult = pattern.matcher(staticInput).replaceAll(JANINO_REPLACEMENT);
        Assertions.assertEquals(staticExpected, staticResult);

        log.info("Janino import replacement test passed");
    }

    @ParameterizedTest
    @CsvSource({
        "import com.google.common.collect.Lists;, import org.apache.seatunnel.shade.com.google.common.collect.Lists;",
        "import static com.google.common.base.Preconditions.checkNotNull;, import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkNotNull;",
        "import org.eclipse.jetty.server.Server;, import org.apache.seatunnel.shade.org.eclipse.jetty.server.Server;",
        "import static org.eclipse.jetty.http.HttpStatus.OK_200;, import static org.apache.seatunnel.shade.org.eclipse.jetty.http.HttpStatus.OK_200;",
        "import com.zaxxer.hikari.HikariDataSource;, import org.apache.seatunnel.shade.com.zaxxer.hikari.HikariDataSource;",
        "import static com.zaxxer.hikari.HikariConfig.MINIMUM_IDLE;, import static org.apache.seatunnel.shade.com.zaxxer.hikari.HikariConfig.MINIMUM_IDLE;",
        "import org.codehaus.janino.ExpressionEvaluator;, import org.apache.seatunnel.shade.org.codehaus.janino.ExpressionEvaluator;",
        "import org.codehaus.commons.compiler.CompileException;, import org.apache.seatunnel.shade.org.codehaus.commons.compiler.CompileException;"
    })
    public void testAllImportReplacements(String input, String expected) {
        String result = input + "\n";

        // Apply all replacement patterns
        result = Pattern.compile(GUAVA_REGEX).matcher(result).replaceAll(GUAVA_REPLACEMENT);
        result = Pattern.compile(JETTY_REGEX).matcher(result).replaceAll(JETTY_REPLACEMENT);
        result = Pattern.compile(HIKARI_REGEX).matcher(result).replaceAll(HIKARI_REPLACEMENT);
        result = Pattern.compile(JANINO_REGEX).matcher(result).replaceAll(JANINO_REPLACEMENT);

        // Remove trailing newline for comparison
        result = result.trim();

        Assertions.assertEquals(expected, result);
    }

    @Test
    public void testNoReplacementForAlreadyShadedImports() {
        // Test that already shaded imports are not modified
        String[] shadedImports = {
            "import org.apache.seatunnel.shade.com.google.common.collect.Lists;",
            "import org.apache.seatunnel.shade.org.eclipse.jetty.server.Server;",
            "import org.apache.seatunnel.shade.com.zaxxer.hikari.HikariDataSource;",
            "import org.apache.seatunnel.shade.org.codehaus.janino.ExpressionEvaluator;"
        };

        for (String shadedImport : shadedImports) {
            String input = shadedImport + "\n";
            String result = input;

            // Apply all replacement patterns
            result = Pattern.compile(GUAVA_REGEX).matcher(result).replaceAll(GUAVA_REPLACEMENT);
            result = Pattern.compile(JETTY_REGEX).matcher(result).replaceAll(JETTY_REPLACEMENT);
            result = Pattern.compile(HIKARI_REGEX).matcher(result).replaceAll(HIKARI_REPLACEMENT);
            result = Pattern.compile(JANINO_REGEX).matcher(result).replaceAll(JANINO_REPLACEMENT);

            Assertions.assertEquals(
                    input, result, "Already shaded import should not be modified: " + shadedImport);
        }

        log.info("No replacement for already shaded imports test passed");
    }
}
