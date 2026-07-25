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

package org.apache.seatunnel.transform.chunk;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.connector.TableTransform;
import org.apache.seatunnel.api.table.factory.TableTransformFactoryContext;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.transform.common.TransformCommonOptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TextChunkTransformFactoryTest {

    @Test
    public void testFactoryIdentifierAndOptionRule() {
        TextChunkTransformFactory factory = new TextChunkTransformFactory();
        Assertions.assertEquals(TextChunkTransform.PLUGIN_NAME, factory.factoryIdentifier());
        Assertions.assertNotNull(factory.optionRule());
    }

    @Test
    public void testCreateTransformReturnsMultiCatalogTransform() {
        TextChunkTransformFactory factory = new TextChunkTransformFactory();

        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "content"},
                        new SeaTunnelDataType[] {BasicType.INT_TYPE, BasicType.STRING_TYPE});
        CatalogTable catalogTable =
                CatalogTableUtil.getCatalogTable("schema", "default", null, "test", rowType);
        List<CatalogTable> tables = Collections.singletonList(catalogTable);

        ReadonlyConfig config =
                ReadonlyConfig.fromMap(
                        Collections.singletonMap(
                                TextChunkTransformConfig.TEXT_FIELD.key(), "content"));

        TableTransformFactoryContext context =
                new TableTransformFactoryContext(
                        tables, config, Thread.currentThread().getContextClassLoader());

        TableTransform<?> tableTransform = factory.createTransform(context);
        Assertions.assertNotNull(tableTransform);

        SeaTunnelTransform<?> inner = tableTransform.createTransform();
        Assertions.assertNotNull(inner);
        Assertions.assertTrue(inner instanceof TextChunkMultiCatalogTransform);
    }

    @Test
    public void testValidConfigWithTextField() {
        OptionRule rule = new TextChunkTransformFactory().optionRule();
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(TextChunkTransformConfig.TEXT_FIELD.key(), "content");
        Assertions.assertDoesNotThrow(
                () -> ConfigValidator.of(ReadonlyConfig.fromMap(cfg)).validate(rule));
    }

    @Test
    public void testMissingTextFieldFails() {
        OptionRule rule = new TextChunkTransformFactory().optionRule();
        Map<String, Object> cfg = new HashMap<>();
        OptionValidationException ex =
                Assertions.assertThrows(
                        OptionValidationException.class,
                        () -> ConfigValidator.of(ReadonlyConfig.fromMap(cfg)).validate(rule));
        Assertions.assertTrue(ex.getMessage().contains("text_field"), ex.getMessage());
    }

    @Test
    public void testBlankTextFieldFailsAtCheckTime() {
        OptionRule rule = new TextChunkTransformFactory().optionRule();
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(TextChunkTransformConfig.TEXT_FIELD.key(), "");
        OptionValidationException ex =
                Assertions.assertThrows(
                        OptionValidationException.class,
                        () -> ConfigValidator.of(ReadonlyConfig.fromMap(cfg)).validate(rule));
        Assertions.assertTrue(ex.getMessage().contains("text_field"), ex.getMessage());
    }

    @Test
    public void testNonPositiveChunkSizeFails() {
        OptionRule rule = new TextChunkTransformFactory().optionRule();
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(TextChunkTransformConfig.TEXT_FIELD.key(), "content");
        cfg.put(TextChunkTransformConfig.CHUNK_SIZE.key(), 0);
        OptionValidationException ex =
                Assertions.assertThrows(
                        OptionValidationException.class,
                        () -> ConfigValidator.of(ReadonlyConfig.fromMap(cfg)).validate(rule));
        Assertions.assertTrue(ex.getMessage().contains("chunk_size"), ex.getMessage());
    }

    @Test
    public void testNegativeOverlapFails() {
        OptionRule rule = new TextChunkTransformFactory().optionRule();
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(TextChunkTransformConfig.TEXT_FIELD.key(), "content");
        cfg.put(TextChunkTransformConfig.OVERLAP_SIZE.key(), -1);
        OptionValidationException ex =
                Assertions.assertThrows(
                        OptionValidationException.class,
                        () -> ConfigValidator.of(ReadonlyConfig.fromMap(cfg)).validate(rule));
        Assertions.assertTrue(ex.getMessage().contains("overlap_size"), ex.getMessage());
    }

    @Test
    public void testOverlapNotLessThanChunkSizeFails() {
        OptionRule rule = new TextChunkTransformFactory().optionRule();
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(TextChunkTransformConfig.TEXT_FIELD.key(), "content");
        cfg.put(TextChunkTransformConfig.CHUNK_SIZE.key(), 100);
        cfg.put(TextChunkTransformConfig.OVERLAP_SIZE.key(), 100);
        OptionValidationException ex =
                Assertions.assertThrows(
                        OptionValidationException.class,
                        () -> ConfigValidator.of(ReadonlyConfig.fromMap(cfg)).validate(rule));
        Assertions.assertTrue(ex.getMessage().contains("overlap_size"), ex.getMessage());
    }

    @Test
    public void testTopLevelDuplicateOutputAndIndexFieldFails() {
        OptionRule rule = new TextChunkTransformFactory().optionRule();
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(TextChunkTransformConfig.TEXT_FIELD.key(), "content");
        cfg.put(TextChunkTransformConfig.OUTPUT_FIELD.key(), "same");
        cfg.put(TextChunkTransformConfig.CHUNK_INDEX_FIELD.key(), "same");
        OptionValidationException ex =
                Assertions.assertThrows(
                        OptionValidationException.class,
                        () -> ConfigValidator.of(ReadonlyConfig.fromMap(cfg)).validate(rule));
        Assertions.assertTrue(
                ex.getMessage().contains("output_field and chunk_index_field must be different"),
                ex.getMessage());
    }

    @Test
    public void testTopLevelIndexFieldCollidingWithDefaultOutputFails() {
        OptionRule rule = new TextChunkTransformFactory().optionRule();
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(TextChunkTransformConfig.TEXT_FIELD.key(), "content");
        cfg.put(TextChunkTransformConfig.CHUNK_INDEX_FIELD.key(), "chunk");
        OptionValidationException ex =
                Assertions.assertThrows(
                        OptionValidationException.class,
                        () -> ConfigValidator.of(ReadonlyConfig.fromMap(cfg)).validate(rule));
        Assertions.assertTrue(
                ex.getMessage().contains("output_field and chunk_index_field must be different"),
                ex.getMessage());
    }

    @Test
    public void testInvalidPerTableRuleFailsAtCheckTime() {
        OptionRule rule = new TextChunkTransformFactory().optionRule();
        Map<String, Object> badTable = new HashMap<>();
        badTable.put(TransformCommonOptions.TABLE_PATH.key(), "db.tbl");
        badTable.put(TextChunkTransformConfig.TEXT_FIELD.key(), "content");
        badTable.put(TextChunkTransformConfig.CHUNK_SIZE.key(), 100);
        badTable.put(TextChunkTransformConfig.OVERLAP_SIZE.key(), 100); // overlap >= chunk_size
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(TextChunkTransformConfig.TEXT_FIELD.key(), "content");
        cfg.put(TransformCommonOptions.MULTI_TABLES.key(), Arrays.asList(badTable));
        OptionValidationException ex =
                Assertions.assertThrows(
                        OptionValidationException.class,
                        () -> ConfigValidator.of(ReadonlyConfig.fromMap(cfg)).validate(rule));
        Assertions.assertTrue(ex.getMessage().contains("overlap_size"), ex.getMessage());
    }

    @Test
    public void testPerTableMissingTextFieldFailsAtCheckTime() {
        OptionRule rule = new TextChunkTransformFactory().optionRule();
        Map<String, Object> table = new HashMap<>();
        table.put(TransformCommonOptions.TABLE_PATH.key(), "db.tbl");
        table.put(TextChunkTransformConfig.CHUNK_SIZE.key(), 100); // text_field omitted
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(TextChunkTransformConfig.TEXT_FIELD.key(), "content"); // top-level present
        cfg.put(TransformCommonOptions.MULTI_TABLES.key(), Arrays.asList(table));
        OptionValidationException ex =
                Assertions.assertThrows(
                        OptionValidationException.class,
                        () -> ConfigValidator.of(ReadonlyConfig.fromMap(cfg)).validate(rule));
        Assertions.assertTrue(ex.getMessage().contains("text_field"), ex.getMessage());
    }

    @Test
    public void testPerTableBlankTextFieldFailsAtCheckTime() {
        OptionRule rule = new TextChunkTransformFactory().optionRule();
        Map<String, Object> table = new HashMap<>();
        table.put(TransformCommonOptions.TABLE_PATH.key(), "db.tbl");
        table.put(TextChunkTransformConfig.TEXT_FIELD.key(), "  ");
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(TextChunkTransformConfig.TEXT_FIELD.key(), "content");
        cfg.put(TransformCommonOptions.MULTI_TABLES.key(), Arrays.asList(table));
        OptionValidationException ex =
                Assertions.assertThrows(
                        OptionValidationException.class,
                        () -> ConfigValidator.of(ReadonlyConfig.fromMap(cfg)).validate(rule));
        Assertions.assertTrue(ex.getMessage().contains("text_field"), ex.getMessage());
    }

    @Test
    public void testValidPerTableRulePasses() {
        OptionRule rule = new TextChunkTransformFactory().optionRule();
        Map<String, Object> goodTable = new HashMap<>();
        goodTable.put(TransformCommonOptions.TABLE_PATH.key(), "db.tbl");
        goodTable.put(TextChunkTransformConfig.TEXT_FIELD.key(), "content");
        goodTable.put(TextChunkTransformConfig.CHUNK_SIZE.key(), 100);
        goodTable.put(TextChunkTransformConfig.OVERLAP_SIZE.key(), 20);
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(TextChunkTransformConfig.TEXT_FIELD.key(), "content");
        cfg.put(TransformCommonOptions.MULTI_TABLES.key(), Arrays.asList(goodTable));
        Assertions.assertDoesNotThrow(
                () -> ConfigValidator.of(ReadonlyConfig.fromMap(cfg)).validate(rule));
    }
}
