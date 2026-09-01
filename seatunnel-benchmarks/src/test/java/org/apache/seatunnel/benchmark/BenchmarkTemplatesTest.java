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

package org.apache.seatunnel.benchmark;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BenchmarkTemplatesTest {

    @Test
    void shouldLoadUtf8ClasspathTemplateWithTrailingNewline() {
        String template = BenchmarkTemplates.load("/benchmark/engine.yaml.template");

        assertTrue(template.contains("seatunnel:"));
        assertTrue(template.endsWith("\n"));
    }

    @Test
    void shouldRenderAllNamedPlaceholders() {
        String rendered =
                BenchmarkTemplates.render(
                        "name={{name}}, count={{count}}", "name", "storage", "count", 2);

        assertEquals("name=storage, count=2", rendered);
    }

    @Test
    void shouldRejectInvalidReplacementPairs() {
        assertThrows(
                IllegalArgumentException.class,
                () -> BenchmarkTemplates.render("{{name}}", "name"));
    }

    @Test
    void shouldRejectUnresolvedPlaceholders() {
        assertThrows(
                IllegalStateException.class,
                () -> BenchmarkTemplates.render("{{name}}-{{missing}}", "name", "storage"));
    }

    @Test
    void shouldRejectMissingClasspathTemplate() {
        assertThrows(
                IllegalStateException.class,
                () -> BenchmarkTemplates.load("/benchmark/missing.template"));
    }
}
