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

package org.apache.seatunnel.transform;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.transform.filterrowkind.FilterRowKindTransform;
import org.apache.seatunnel.transform.filterrowkind.FilterRowKindTransformFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

class FilterRowKindTransformFactoryTest {

    private final OptionRule rule = new FilterRowKindTransformFactory().optionRule();

    private void validate(Map<String, Object> config) {
        ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(rule);
    }

    private void assertValidationFails(Map<String, Object> config, String... optionKeys) {
        OptionValidationException exception =
                Assertions.assertThrows(OptionValidationException.class, () -> validate(config));
        for (String optionKey : optionKeys) {
            Assertions.assertTrue(
                    exception.getMessage().contains(optionKey),
                    "Should mention " + optionKey + ": " + exception.getMessage());
        }
    }

    @Test
    void testIncludeKindsValid() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("include_kinds", Arrays.asList("INSERT", "UPDATE_AFTER"));
        Assertions.assertDoesNotThrow(() -> validate(cfg));
    }

    @Test
    void testExcludeKindsValid() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("exclude_kinds", Arrays.asList("DELETE", "UPDATE_BEFORE"));
        Assertions.assertDoesNotThrow(() -> validate(cfg));
    }

    @Test
    void testNeitherKindsFails() {
        Map<String, Object> cfg = new HashMap<>();
        assertValidationFails(cfg, "include_kinds", "exclude_kinds");
    }

    @Test
    void testBothKindsFail() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("include_kinds", Arrays.asList("INSERT"));
        cfg.put("exclude_kinds", Arrays.asList("DELETE"));
        assertValidationFails(cfg, "include_kinds", "exclude_kinds");
    }

    @Test
    void testIncludeKindsEmptyFails() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("include_kinds", Collections.emptyList());
        assertValidationFails(cfg, "include_kinds");
    }

    @Test
    void testExcludeKindsEmptyFails() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("exclude_kinds", Collections.emptyList());
        assertValidationFails(cfg, "exclude_kinds");
    }

    @Test
    void testDirectConstructionWithEmptyKindsFailsOnTransform() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("exclude_kinds", Collections.emptyList());
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id"}, new SeaTunnelDataType[] {BasicType.INT_TYPE});
        CatalogTable catalogTable = CatalogTableUtil.getCatalogTable("test", rowType);
        FilterRowKindTransform transform =
                new FilterRowKindTransform(ReadonlyConfig.fromMap(cfg), catalogTable);

        SeaTunnelRuntimeException exception =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () -> transform.map(new SeaTunnelRow(new Object[] {1})));
        Assertions.assertTrue(
                exception.getMessage().contains("Either excludeKinds or includeKinds"),
                "Should explain the required options: " + exception.getMessage());
    }
}
