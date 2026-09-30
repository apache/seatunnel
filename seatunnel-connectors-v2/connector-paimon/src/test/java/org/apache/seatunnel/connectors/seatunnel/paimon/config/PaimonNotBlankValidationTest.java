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

    // ── helpers ──────────────────────────────────────────────────────────────

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

    // ── notBlank tests ────────────────────────────────────────────────────────

    // ── Sink: warehouse ──────────────────────────────────────────────────────

    @Test
    void notBlank_sinkValidConfig() {
        Assertions.assertDoesNotThrow(
                () -> validateSink(validSink()),
                "Valid sink config must pass after notBlank change");
    }

    @Test
    void notBlank_sinkMissingWarehouse() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(PaimonSinkOptions.DATABASE.key(), "db");
        cfg.put(PaimonSinkOptions.TABLE.key(), "t");
        Assertions.assertThrows(OptionValidationException.class, () -> validateSink(cfg));
    }

    @Test
    void notBlank_sinkEmptyWarehouse() {
        Map<String, Object> cfg = validSink();
        cfg.put(PaimonSinkOptions.WAREHOUSE.key(), "");
        Assertions.assertThrows(OptionValidationException.class, () -> validateSink(cfg));
    }

    @Test
    void notBlank_sinkWhitespaceWarehouse() {
        Map<String, Object> cfg = validSink();
        cfg.put(PaimonSinkOptions.WAREHOUSE.key(), "   ");
        Assertions.assertThrows(OptionValidationException.class, () -> validateSink(cfg));
    }

    @Test
    void notBlank_sinkPaddedWarehousePasses() {
        Map<String, Object> cfg = validSink();
        cfg.put(PaimonSinkOptions.WAREHOUSE.key(), " file:///tmp/paimon ");
        Assertions.assertDoesNotThrow(
                () -> validateSink(cfg),
                "Padded warehouse (' file:///tmp/paimon ') must pass — notBlank trims internally");
    }

    // ── Sink: database ───────────────────────────────────────────────────────

    @Test
    void notBlank_sinkEmptyDatabase() {
        Map<String, Object> cfg = validSink();
        cfg.put(PaimonSinkOptions.DATABASE.key(), "");
        Assertions.assertThrows(OptionValidationException.class, () -> validateSink(cfg));
    }

    @Test
    void notBlank_sinkWhitespaceDatabase() {
        Map<String, Object> cfg = validSink();
        cfg.put(PaimonSinkOptions.DATABASE.key(), "  \t  ");
        Assertions.assertThrows(OptionValidationException.class, () -> validateSink(cfg));
    }

    @Test
    void notBlank_sinkPaddedDatabasePasses() {
        Map<String, Object> cfg = validSink();
        cfg.put(PaimonSinkOptions.DATABASE.key(), " db ");
        Assertions.assertDoesNotThrow(
                () -> validateSink(cfg), "Padded database ' db ' must pass notBlank");
    }

    // ── Sink: table ──────────────────────────────────────────────────────────

    @Test
    void notBlank_sinkEmptyTable() {
        Map<String, Object> cfg = validSink();
        cfg.put(PaimonSinkOptions.TABLE.key(), "");
        Assertions.assertThrows(OptionValidationException.class, () -> validateSink(cfg));
    }

    @Test
    void notBlank_sinkWhitespaceTable() {
        Map<String, Object> cfg = validSink();
        cfg.put(PaimonSinkOptions.TABLE.key(), "   ");
        Assertions.assertThrows(OptionValidationException.class, () -> validateSink(cfg));
    }

    @Test
    void notBlank_sinkPaddedTablePasses() {
        Map<String, Object> cfg = validSink();
        cfg.put(PaimonSinkOptions.TABLE.key(), " t ");
        Assertions.assertDoesNotThrow(
                () -> validateSink(cfg), "Padded table ' t ' must pass notBlank");
    }

    // ── Source: warehouse ─────────────────────────────────────────────────────

    @Test
    void notBlank_sourceValidConfig() {
        Assertions.assertDoesNotThrow(
                () -> validateSource(validSource()),
                "Valid source config must pass after notBlank change");
    }

    @Test
    void notBlank_sourceMissingWarehouse() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(PaimonBaseOptions.TABLE.key(), "t");
        Assertions.assertThrows(OptionValidationException.class, () -> validateSource(cfg));
    }

    @Test
    void notBlank_sourceEmptyWarehouse() {
        Map<String, Object> cfg = validSource();
        cfg.put(PaimonBaseOptions.WAREHOUSE.key(), "");
        Assertions.assertThrows(OptionValidationException.class, () -> validateSource(cfg));
    }

    @Test
    void notBlank_sourceWhitespaceWarehouse() {
        Map<String, Object> cfg = validSource();
        cfg.put(PaimonBaseOptions.WAREHOUSE.key(), "   ");
        Assertions.assertThrows(OptionValidationException.class, () -> validateSource(cfg));
    }

    @Test
    void notBlank_sourcePaddedWarehousePasses() {
        Map<String, Object> cfg = validSource();
        cfg.put(PaimonBaseOptions.WAREHOUSE.key(), " file:///tmp/paimon ");
        Assertions.assertDoesNotThrow(
                () -> validateSource(cfg), "Padded source warehouse must pass notBlank");
    }

    // ── Catalog: warehouse, database, table ───────────────────────────────────

    @Test
    void notBlank_catalogValidConfig() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(PaimonBaseOptions.WAREHOUSE.key(), "file:///tmp/paimon");
        cfg.put(PaimonBaseOptions.DATABASE.key(), "db");
        cfg.put(PaimonBaseOptions.TABLE.key(), "t");
        Assertions.assertDoesNotThrow(
                () -> validateCatalog(cfg), "Valid catalog config must pass after notBlank change");
    }

    @Test
    void notBlank_catalogEmptyWarehouse() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(PaimonBaseOptions.WAREHOUSE.key(), "");
        cfg.put(PaimonBaseOptions.DATABASE.key(), "db");
        cfg.put(PaimonBaseOptions.TABLE.key(), "t");
        Assertions.assertThrows(OptionValidationException.class, () -> validateCatalog(cfg));
    }

    @Test
    void notBlank_catalogWhitespaceDatabase() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(PaimonBaseOptions.WAREHOUSE.key(), "file:///tmp/paimon");
        cfg.put(PaimonBaseOptions.DATABASE.key(), "  ");
        cfg.put(PaimonBaseOptions.TABLE.key(), "t");
        Assertions.assertThrows(OptionValidationException.class, () -> validateCatalog(cfg));
    }

    @Test
    void notBlank_catalogEmptyTable() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(PaimonBaseOptions.WAREHOUSE.key(), "file:///tmp/paimon");
        cfg.put(PaimonBaseOptions.DATABASE.key(), "db");
        cfg.put(PaimonBaseOptions.TABLE.key(), "");
        Assertions.assertThrows(OptionValidationException.class, () -> validateCatalog(cfg));
    }
}
