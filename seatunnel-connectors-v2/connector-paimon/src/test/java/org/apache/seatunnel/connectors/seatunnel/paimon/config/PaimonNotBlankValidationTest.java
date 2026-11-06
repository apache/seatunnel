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

package org.apache.seatunnel.connectors.seatunnel.paimon.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.connectors.seatunnel.paimon.catalog.PaimonCatalogFactory;
import org.apache.seatunnel.connectors.seatunnel.paimon.sink.PaimonSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.paimon.source.PaimonSourceFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Validates that {@link PaimonSourceFactory}, {@link PaimonSinkFactory}, and {@link
 * PaimonCatalogFactory} reject blank values for their required string options, per #11007.
 */
class PaimonNotBlankValidationTest {

    private static void validateSink(Map<String, Object> cfg) {
        ConfigValidator.of(ReadonlyConfig.fromMap(cfg))
                .validate(new PaimonSinkFactory().optionRule());
    }

    private static void validateSource(Map<String, Object> cfg) {
        ConfigValidator.of(ReadonlyConfig.fromMap(cfg))
                .validate(new PaimonSourceFactory().optionRule());
    }

    private static void validateCatalog(Map<String, Object> cfg) {
        ConfigValidator.of(ReadonlyConfig.fromMap(cfg))
                .validate(new PaimonCatalogFactory().optionRule());
    }

    private static Map<String, Object> validSink() {
        Map<String, Object> m = new HashMap<>();
        m.put(PaimonSinkOptions.WAREHOUSE.key(), "file:///tmp/paimon");
        m.put(PaimonSinkOptions.DATABASE.key(), "db");
        m.put(PaimonSinkOptions.TABLE.key(), "t");
        return m;
    }

    private static Map<String, Object> validSource() {
        Map<String, Object> m = new HashMap<>();
        m.put(PaimonBaseOptions.WAREHOUSE.key(), "file:///tmp/paimon");
        m.put(PaimonBaseOptions.TABLE.key(), "t");
        return m;
    }

    @Test
    void notBlankSinkValidConfig() {
        Assertions.assertDoesNotThrow(
                () -> validateSink(validSink()),
                "Valid sink config must pass after notBlank change");
    }

    @Test
    void notBlankSinkMissingWarehouse() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(PaimonSinkOptions.DATABASE.key(), "db");
        cfg.put(PaimonSinkOptions.TABLE.key(), "t");
        OptionValidationException ex =
                Assertions.assertThrows(OptionValidationException.class, () -> validateSink(cfg));
        Assertions.assertTrue(
                ex.getMessage().contains("warehouse"),
                "Expected failure to mention 'warehouse', got: " + ex.getMessage());
    }

    @Test
    void notBlankSinkEmptyWarehouse() {
        Map<String, Object> cfg = validSink();
        cfg.put(PaimonSinkOptions.WAREHOUSE.key(), "");
        OptionValidationException ex =
                Assertions.assertThrows(OptionValidationException.class, () -> validateSink(cfg));
        Assertions.assertTrue(
                ex.getMessage().contains("warehouse"),
                "Expected failure to mention 'warehouse', got: " + ex.getMessage());
    }

    @Test
    void notBlankSinkWhitespaceWarehouse() {
        Map<String, Object> cfg = validSink();
        cfg.put(PaimonSinkOptions.WAREHOUSE.key(), "   ");
        OptionValidationException ex =
                Assertions.assertThrows(OptionValidationException.class, () -> validateSink(cfg));
        Assertions.assertTrue(
                ex.getMessage().contains("warehouse"),
                "Expected failure to mention 'warehouse', got: " + ex.getMessage());
    }

    @Test
    void notBlankSinkPaddedWarehousePasses() {
        Map<String, Object> cfg = validSink();
        cfg.put(PaimonSinkOptions.WAREHOUSE.key(), " file:///tmp/paimon ");
        Assertions.assertDoesNotThrow(
                () -> validateSink(cfg),
                "Padded warehouse (' file:///tmp/paimon ') must pass — notBlank trims internally");
    }

    @Test
    void notBlankSinkEmptyDatabase() {
        Map<String, Object> cfg = validSink();
        cfg.put(PaimonSinkOptions.DATABASE.key(), "");
        OptionValidationException ex =
                Assertions.assertThrows(OptionValidationException.class, () -> validateSink(cfg));
        Assertions.assertTrue(
                ex.getMessage().contains("database"),
                "Expected failure to mention 'database', got: " + ex.getMessage());
    }

    @Test
    void notBlankSinkWhitespaceDatabase() {
        Map<String, Object> cfg = validSink();
        cfg.put(PaimonSinkOptions.DATABASE.key(), "  \t  ");
        OptionValidationException ex =
                Assertions.assertThrows(OptionValidationException.class, () -> validateSink(cfg));
        Assertions.assertTrue(
                ex.getMessage().contains("database"),
                "Expected failure to mention 'database', got: " + ex.getMessage());
    }

    @Test
    void notBlankSinkPaddedDatabasePasses() {
        Map<String, Object> cfg = validSink();
        cfg.put(PaimonSinkOptions.DATABASE.key(), " db ");
        Assertions.assertDoesNotThrow(
                () -> validateSink(cfg), "Padded database ' db ' must pass notBlank");
    }

    @Test
    void notBlankSinkEmptyTable() {
        Map<String, Object> cfg = validSink();
        cfg.put(PaimonSinkOptions.TABLE.key(), "");
        OptionValidationException ex =
                Assertions.assertThrows(OptionValidationException.class, () -> validateSink(cfg));
        Assertions.assertTrue(
                ex.getMessage().contains("table"),
                "Expected failure to mention 'table', got: " + ex.getMessage());
    }

    @Test
    void notBlankSinkWhitespaceTable() {
        Map<String, Object> cfg = validSink();
        cfg.put(PaimonSinkOptions.TABLE.key(), "   ");
        OptionValidationException ex =
                Assertions.assertThrows(OptionValidationException.class, () -> validateSink(cfg));
        Assertions.assertTrue(
                ex.getMessage().contains("table"),
                "Expected failure to mention 'table', got: " + ex.getMessage());
    }

    @Test
    void notBlankSinkPaddedTablePasses() {
        Map<String, Object> cfg = validSink();
        cfg.put(PaimonSinkOptions.TABLE.key(), " t ");
        Assertions.assertDoesNotThrow(
                () -> validateSink(cfg), "Padded table ' t ' must pass notBlank");
    }

    @Test
    void notBlankSourceValidConfig() {
        Assertions.assertDoesNotThrow(
                () -> validateSource(validSource()),
                "Valid source config must pass after notBlank change");
    }

    @Test
    void notBlankSourceMissingWarehouse() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(PaimonBaseOptions.TABLE.key(), "t");
        OptionValidationException ex =
                Assertions.assertThrows(OptionValidationException.class, () -> validateSource(cfg));
        Assertions.assertTrue(
                ex.getMessage().contains("warehouse"),
                "Expected failure to mention 'warehouse', got: " + ex.getMessage());
    }

    @Test
    void notBlankSourceEmptyWarehouse() {
        Map<String, Object> cfg = validSource();
        cfg.put(PaimonBaseOptions.WAREHOUSE.key(), "");
        OptionValidationException ex =
                Assertions.assertThrows(OptionValidationException.class, () -> validateSource(cfg));
        Assertions.assertTrue(
                ex.getMessage().contains("warehouse"),
                "Expected failure to mention 'warehouse', got: " + ex.getMessage());
    }

    @Test
    void notBlankSourceWhitespaceWarehouse() {
        Map<String, Object> cfg = validSource();
        cfg.put(PaimonBaseOptions.WAREHOUSE.key(), "   ");
        OptionValidationException ex =
                Assertions.assertThrows(OptionValidationException.class, () -> validateSource(cfg));
        Assertions.assertTrue(
                ex.getMessage().contains("warehouse"),
                "Expected failure to mention 'warehouse', got: " + ex.getMessage());
    }

    @Test
    void notBlankSourcePaddedWarehousePasses() {
        Map<String, Object> cfg = validSource();
        cfg.put(PaimonBaseOptions.WAREHOUSE.key(), " file:///tmp/paimon ");
        Assertions.assertDoesNotThrow(
                () -> validateSource(cfg), "Padded source warehouse must pass notBlank");
    }

    @Test
    void notBlankCatalogValidConfig() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(PaimonBaseOptions.WAREHOUSE.key(), "file:///tmp/paimon");
        cfg.put(PaimonBaseOptions.DATABASE.key(), "db");
        cfg.put(PaimonBaseOptions.TABLE.key(), "t");
        Assertions.assertDoesNotThrow(
                () -> validateCatalog(cfg), "Valid catalog config must pass after notBlank change");
    }

    @Test
    void notBlankCatalogEmptyWarehouse() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(PaimonBaseOptions.WAREHOUSE.key(), "");
        cfg.put(PaimonBaseOptions.DATABASE.key(), "db");
        cfg.put(PaimonBaseOptions.TABLE.key(), "t");
        OptionValidationException ex =
                Assertions.assertThrows(
                        OptionValidationException.class, () -> validateCatalog(cfg));
        Assertions.assertTrue(
                ex.getMessage().contains("warehouse"),
                "Expected failure to mention 'warehouse', got: " + ex.getMessage());
    }

    @Test
    void notBlankCatalogWhitespaceDatabase() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(PaimonBaseOptions.WAREHOUSE.key(), "file:///tmp/paimon");
        cfg.put(PaimonBaseOptions.DATABASE.key(), "  ");
        cfg.put(PaimonBaseOptions.TABLE.key(), "t");
        OptionValidationException ex =
                Assertions.assertThrows(
                        OptionValidationException.class, () -> validateCatalog(cfg));
        Assertions.assertTrue(
                ex.getMessage().contains("database"),
                "Expected failure to mention 'database', got: " + ex.getMessage());
    }

    @Test
    void notBlankCatalogEmptyTable() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(PaimonBaseOptions.WAREHOUSE.key(), "file:///tmp/paimon");
        cfg.put(PaimonBaseOptions.DATABASE.key(), "db");
        cfg.put(PaimonBaseOptions.TABLE.key(), "");
        OptionValidationException ex =
                Assertions.assertThrows(
                        OptionValidationException.class, () -> validateCatalog(cfg));
        Assertions.assertTrue(
                ex.getMessage().contains("table"),
                "Expected failure to mention 'table', got: " + ex.getMessage());
    }
}
