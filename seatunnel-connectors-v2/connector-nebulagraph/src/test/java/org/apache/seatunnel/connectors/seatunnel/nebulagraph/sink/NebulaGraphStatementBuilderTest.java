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

package org.apache.seatunnel.connectors.seatunnel.nebulagraph.sink;

import org.apache.seatunnel.connectors.seatunnel.nebulagraph.config.NebulaGraphWriteMode;
import org.apache.seatunnel.connectors.seatunnel.nebulagraph.exception.NebulaGraphConnectorException;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class NebulaGraphStatementBuilderTest {

    @Test
    void buildsParameterizedBatchInsert() {
        NebulaGraphStatementBuilder builder =
                new NebulaGraphStatementBuilder(
                        "person", Arrays.asList("name", "age"), NebulaGraphWriteMode.INSERT);

        NebulaGraphWriteRequest request =
                builder.build(Arrays.asList(vertex("p1", "Alice", 31L), vertex("p2", "Bob", 29L)));

        assertEquals(
                "INSERT VERTEX IF NOT EXISTS `person` (`name`,`age`) VALUES "
                        + "\"p1\":($value_0_0,$value_0_1),\"p2\":($value_1_0,$value_1_1)",
                request.getStatement());
        assertEquals(31L, request.getParameters().get("value_0_1"));
        assertFalse(request.getParameters().containsKey("vid_0"));
        assertFalse(request.getStatement().contains("Alice"));
    }

    @Test
    void buildsParameterizedBatchUpdate() {
        NebulaGraphStatementBuilder builder =
                new NebulaGraphStatementBuilder(
                        "person", Arrays.asList("name", "age"), NebulaGraphWriteMode.UPDATE);

        NebulaGraphWriteRequest request =
                builder.build(Arrays.asList(vertex("p1", "Alice", 31L), vertex("p2", "Bob", 29L)));

        assertEquals(
                "UPDATE VERTEX ON `person` \"p1\" SET `name`=$value_0_0,`age`=$value_0_1;"
                        + "UPDATE VERTEX ON `person` \"p2\" SET `name`=$value_1_0,`age`=$value_1_1",
                request.getStatement());
        assertEquals("Bob", request.getParameters().get("value_1_0"));
    }

    @Test
    void escapesStringVertexIdsAndFormatsIntegerVertexIds() {
        NebulaGraphStatementBuilder builder =
                new NebulaGraphStatementBuilder(
                        "person", Arrays.asList("name", "age"), NebulaGraphWriteMode.INSERT);

        NebulaGraphWriteRequest request =
                builder.build(
                        Arrays.asList(vertex("quote\"slash\\line\n", "Alice", 31L), vertex(42L)));

        assertEquals(
                "INSERT VERTEX IF NOT EXISTS `person` (`name`,`age`) VALUES "
                        + "\"quote\\\"slash\\\\line\\n\":($value_0_0,$value_0_1),"
                        + "42:($value_1_0,$value_1_1)",
                request.getStatement());
    }

    @Test
    void rejectsUnsupportedControlCharactersInVertexIds() {
        NebulaGraphStatementBuilder builder =
                new NebulaGraphStatementBuilder(
                        "person", Arrays.asList("name", "age"), NebulaGraphWriteMode.INSERT);

        assertThrows(
                IllegalArgumentException.class,
                () -> builder.build(Arrays.asList(vertex("person\u0000", "Alice", 31L))));
    }

    @Test
    void rejectsUnsafePropertyNames() {
        assertThrows(
                NebulaGraphConnectorException.class,
                () ->
                        new NebulaGraphStatementBuilder(
                                "person",
                                Arrays.asList("name`, admin=true; --"),
                                NebulaGraphWriteMode.INSERT));
    }

    private static NebulaGraphVertex vertex(String vid, String name, long age) {
        Map<String, Object> properties = new LinkedHashMap<>();
        properties.put("name", name);
        properties.put("age", age);
        return new NebulaGraphVertex(vid, properties);
    }

    private static NebulaGraphVertex vertex(long vid) {
        Map<String, Object> properties = new LinkedHashMap<>();
        properties.put("name", "Alice");
        properties.put("age", 31L);
        return new NebulaGraphVertex(vid, properties);
    }
}
