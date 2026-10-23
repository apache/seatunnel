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

package org.apache.seatunnel.connectors.seatunnel.hugegraph.utils;

import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.MappingConfig;

import org.apache.hugegraph.structure.constant.Frequency;
import org.apache.hugegraph.structure.constant.IdStrategy;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins the nullable-by-default semantics that {@link SchemaManager#computeNullableKeys} feeds into
 * HugeGraph label creation. HugeGraph server rejects any insert row that omits a non-nullable
 * property, so a regression here silently breaks partial-property writes.
 */
class SchemaManagerNullableTest {

    @Test
    void defaultsAllNonKeyPropertiesToNullableForVertex() {
        MappingConfig m = vertexPrimaryKey("person", "id");
        Set<String> props = setOf("id", "name", "age");

        List<String> result = SchemaManager.computeNullableKeys(m, props);

        assertEquals(setOf("name", "age"), new HashSet<>(result));
    }

    @Test
    void excludesPrimaryKeyFromDefault() {
        // Even without any explicit config, the PK must not appear in nullableKeys —
        // HugeGraph server rejects the label-create call otherwise.
        MappingConfig m = vertexPrimaryKey("person", "id");
        Set<String> props = setOf("id", "name");

        List<String> result = SchemaManager.computeNullableKeys(m, props);

        assertTrue(result.contains("name"));
        assertTrue(!result.contains("id"));
    }

    @Test
    void excludesSortKeysFromDefaultForMultipleEdge() {
        MappingConfig m = new MappingConfig();
        m.setType(MappingConfig.LabelType.EDGE);
        m.setLabel("visits");
        m.setFrequency(Frequency.MULTIPLE);
        m.setSortKeys(Collections.singletonList("visited_at"));
        Set<String> props = setOf("visited_at", "device");

        List<String> result = SchemaManager.computeNullableKeys(m, props);

        assertEquals(Collections.singletonList("device"), result);
    }

    @Test
    void explicitNullableKeysWinAndAreFiltered() {
        // Explicit user list is respected verbatim, minus keys and unknown props.
        MappingConfig m = vertexPrimaryKey("person", "id");
        m.setNullableKeys(Arrays.asList("name", "id", "missing"));
        Set<String> props = setOf("id", "name", "age");

        List<String> result = SchemaManager.computeNullableKeys(m, props);

        assertEquals(Collections.singletonList("name"), result);
    }

    @Test
    void notNullableKeysCarvesOutRequiredProps() {
        MappingConfig m = vertexPrimaryKey("person", "id");
        m.setNotNullableKeys(Collections.singletonList("name"));
        Set<String> props = setOf("id", "name", "age");

        List<String> result = SchemaManager.computeNullableKeys(m, props);

        assertEquals(Collections.singletonList("age"), sortedOnly(result));
    }

    @Test
    void notNullableKeysHonorsFieldMapping() {
        // User's opt-out list is in source-column names; must be translated through fieldMapping
        // to target property names.
        MappingConfig m = vertexPrimaryKey("person", "src_id");
        HashMap<String, String> fm = new HashMap<>();
        fm.put("src_id", "id");
        fm.put("src_name", "name");
        m.setFieldMapping(fm);
        m.setNotNullableKeys(Collections.singletonList("src_name"));
        Set<String> props = setOf("id", "name", "age");

        List<String> result = SchemaManager.computeNullableKeys(m, props);

        assertEquals(Collections.singletonList("age"), sortedOnly(result));
    }

    @Test
    void explicitNullableKeysHonorsFieldMapping() {
        MappingConfig m = vertexPrimaryKey("person", "src_id");
        HashMap<String, String> fm = new HashMap<>();
        fm.put("src_id", "id");
        fm.put("src_name", "name");
        m.setFieldMapping(fm);
        m.setNullableKeys(Collections.singletonList("src_name"));
        Set<String> props = setOf("id", "name", "age");

        List<String> result = SchemaManager.computeNullableKeys(m, props);

        assertEquals(Collections.singletonList("name"), result);
    }

    @Test
    void emptyPropertySetProducesEmptyResult() {
        MappingConfig m = vertexPrimaryKey("person", "id");
        List<String> result = SchemaManager.computeNullableKeys(m, Collections.emptySet());
        assertTrue(result.isEmpty());
    }

    private static MappingConfig vertexPrimaryKey(String label, String idField) {
        MappingConfig m = new MappingConfig();
        m.setType(MappingConfig.LabelType.VERTEX);
        m.setLabel(label);
        m.setIdStrategy(IdStrategy.PRIMARY_KEY);
        m.setIdFields(Collections.singletonList(idField));
        return m;
    }

    private static Set<String> setOf(String... items) {
        return new HashSet<>(Arrays.asList(items));
    }

    private static List<String> sortedOnly(List<String> list) {
        return new java.util.ArrayList<>(new TreeSet<>(list));
    }
}
