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

package org.apache.seatunnel.connectors.seatunnel.file.source.reader;

import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.file.util.LocalFileSystemConf.LocalConf;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT;

class WordReadStrategyTest {

    @Test
    void readsDocAndDocxWithExpectedContent() throws Exception {
        assertDocumentTitleAndTableContent(getDocFixture());
        assertDocumentTitleAndTableContent(getDocxFixture());
    }

    @Test
    void readsDocAndDocxParagraphsAndTablesInDocumentOrder() throws Exception {
        assertElementsInDocumentOrder(getDocFixture());
        assertElementsInDocumentOrder(getDocxFixture());
    }

    private void assertDocumentTitleAndTableContent(Path file) throws Exception {
        WordReadStrategy strategy = new WordReadStrategy();
        strategy.init(new LocalConf(FS_DEFAULT_NAME_DEFAULT));
        TempCollector collector = new TempCollector();
        strategy.read(file.toString(), "catalog.db.table", collector);

        List<SeaTunnelRow> rows = collector.getRows();
        Assertions.assertFalse(rows.isEmpty());
        Assertions.assertTrue(
                rows.stream()
                        .anyMatch(
                                row ->
                                        String.valueOf(row.getField(3))
                                                .contains(
                                                        "The Essential Guide to Groceries: Shopping, Storing, and Enjoying Food at Home")));
        Assertions.assertTrue(
                rows.stream()
                        .anyMatch(
                                row ->
                                        "table".equals(row.getField(1))
                                                && String.valueOf(row.getField(3))
                                                        .contains("Rich in vitamins and fiber")));
        Assertions.assertEquals("catalog.db.table", rows.get(0).getTableId());
    }

    private void assertElementsInDocumentOrder(Path file) throws Exception {
        WordReadStrategy strategy = new WordReadStrategy();
        strategy.init(new LocalConf(FS_DEFAULT_NAME_DEFAULT));
        TempCollector collector = new TempCollector();
        strategy.read(file.toString(), "catalog.db.table", collector);

        List<SeaTunnelRow> rows = collector.getRows();
        Assertions.assertEquals(68, rows.size());
        Assertions.assertEquals(
                11L, rows.stream().filter(row -> "heading".equals(row.getField(1))).count());
        Assertions.assertEquals(
                56L, rows.stream().filter(row -> "paragraph".equals(row.getField(1))).count());
        Assertions.assertEquals(
                1L, rows.stream().filter(row -> "table".equals(row.getField(1))).count());
        Assertions.assertEquals("heading", rows.get(0).getField(1));
        Assertions.assertEquals(1, rows.get(0).getField(2));
        Assertions.assertTrue(
                rows.stream()
                        .anyMatch(
                                row ->
                                        "paragraph".equals(row.getField(1))
                                                && String.valueOf(row.getField(3))
                                                        .startsWith("Groceries play a vital role")),
                "ordinary body text must not be promoted to a heading");
        Assertions.assertEquals(
                "The Essential Guide to Groceries: Shopping, Storing, and Enjoying Food at Home",
                rows.get(0).getField(3));
        Assertions.assertNull(rows.get(0).getField(6));
        String rootId = String.valueOf(rows.get(0).getField(0));
        String tocId = String.valueOf(rows.get(2).getField(0));
        Assertions.assertEquals(rootId, rows.get(2).getField(6));
        Assertions.assertEquals(rootId, rows.get(12).getField(6));
        Assertions.assertEquals(tocId, rows.get(3).getField(6));
        Assertions.assertEquals(tocId, rows.get(11).getField(6));
        Assertions.assertTrue(
                String.valueOf(rows.get(1).getField(3)).startsWith("Groceries play a vital role"));
        Assertions.assertTrue(String.valueOf(rows.get(16).getField(3)).contains("Fresh Produce"));
        Assertions.assertEquals("1. Introduction", rows.get(12).getField(3));
        Assertions.assertEquals(rootId, rows.get(12).getField(6));
        Assertions.assertEquals(String.valueOf(rows.get(12).getField(0)), rows.get(13).getField(6));
        Assertions.assertEquals("8. Comparison Table", rows.get(63).getField(3));
        Assertions.assertEquals("table", rows.get(64).getField(1));
        Assertions.assertTrue(
                String.valueOf(rows.get(64).getField(3)).contains("Rich in vitamins and fiber"));
        Assertions.assertEquals(String.valueOf(rows.get(63).getField(0)), rows.get(64).getField(6));
        Assertions.assertEquals("9. Conclusion", rows.get(65).getField(3));
        Assertions.assertEquals(rootId, rows.get(65).getField(6));
        Assertions.assertTrue(
                String.valueOf(rows.get(67).getField(3)).contains("Groceries Resource"));
        Set<String> elementIds = new HashSet<>();
        for (int i = 0; i < rows.size(); i++) {
            String elementId = String.valueOf(rows.get(i).getField(0));
            Assertions.assertTrue(elementId.matches("uuid-elem-[0-9a-fA-F-]{36}"));
            Assertions.assertTrue(elementIds.add(elementId), "element_id must be unique");
            Assertions.assertEquals(i + 1, rows.get(i).getField(5));
            Assertions.assertEquals("catalog.db.table", rows.get(i).getTableId());
            Assertions.assertEquals(8, rows.get(i).getArity());
            Assertions.assertNull(rows.get(i).getField(4));
            Assertions.assertTrue(
                    "heading".equals(rows.get(i).getField(1))
                            || "paragraph".equals(rows.get(i).getField(1))
                            || "table".equals(rows.get(i).getField(1)));
            if ("heading".equals(rows.get(i).getField(1))) {
                Assertions.assertNotNull(rows.get(i).getField(2));
            } else {
                Assertions.assertNull(rows.get(i).getField(2));
            }
            Assertions.assertTrue(
                    rows.get(i).getField(7) instanceof String[],
                    "child_ids must be a String[] for row " + (i + 1));
            Object parentId = rows.get(i).getField(6);
            if (parentId != null) {
                SeaTunnelRow parent = findRowById(rows, String.valueOf(parentId));
                Assertions.assertNotNull(parent, "parent_id must reference an existing element");
                Assertions.assertTrue(
                        List.of((String[]) parent.getField(7)).contains(elementId),
                        "parent child_ids must reference its child");
            }
            for (String childId : (String[]) rows.get(i).getField(7)) {
                SeaTunnelRow child = findRowById(rows, childId);
                Assertions.assertNotNull(child, "child_ids must reference an existing element");
                Assertions.assertEquals(elementId, child.getField(6));
            }
        }
        Assertions.assertEquals(rows.size(), elementIds.size());

        SeaTunnelRowType schema = strategy.getSeaTunnelRowTypeInfo(file.toString());
        Assertions.assertArrayEquals(
                new String[] {
                    "element_id",
                    "element_type",
                    "heading_level",
                    "text",
                    "page_number",
                    "position_index",
                    "parent_id",
                    "child_ids"
                },
                schema.getFieldNames());
        Assertions.assertEquals(ArrayType.STRING_ARRAY_TYPE, schema.getFieldType(7));
    }

    @Test
    void appendsStableRagMetadataWhenEnabled() throws Exception {
        Path file = getDocxFixture();
        WordReadStrategy strategy = new WordReadStrategy();
        strategy.setPluginConfig(ConfigFactory.parseString("word_rag_metadata_enabled = true"));
        strategy.init(new LocalConf(FS_DEFAULT_NAME_DEFAULT));
        TempCollector collector = new TempCollector();
        strategy.read(file.toString(), "catalog.db.table", collector);

        SeaTunnelRow row = collector.getRows().get(0);
        SeaTunnelRowType schema = strategy.getSeaTunnelRowTypeInfo(file.toString());
        Assertions.assertEquals(13, schema.getFieldNames().length);
        Assertions.assertEquals(13, row.getArity());
        Assertions.assertEquals(file.toString(), row.getField(8));
        Assertions.assertTrue(String.valueOf(row.getField(9)).startsWith("doc_"));
        Assertions.assertTrue(String.valueOf(row.getField(10)).startsWith("chunk_"));
        Assertions.assertEquals(1, row.getField(11));
        Assertions.assertEquals(64, String.valueOf(row.getField(12)).length());
        Assertions.assertNotNull(row.getOptionsOrNull());
    }

    @Test
    void readsContentControlsAndTreatsOutlineLevelNineAsBodyText() throws Exception {
        Path file =
                Path.of(getClass().getResource("/outline-level-nine-content-control.docx").toURI());
        WordReadStrategy strategy = new WordReadStrategy();
        strategy.init(new LocalConf(FS_DEFAULT_NAME_DEFAULT));
        TempCollector collector = new TempCollector();
        strategy.read(file.toString(), "catalog.db.table", collector);

        List<SeaTunnelRow> rows = collector.getRows();
        Assertions.assertEquals(2, rows.size());
        Assertions.assertEquals("paragraph", rows.get(0).getField(1));
        Assertions.assertEquals("Body text with outline level nine", rows.get(0).getField(3));
        Assertions.assertEquals("paragraph", rows.get(1).getField(1));
        Assertions.assertEquals("Content control text", rows.get(1).getField(3));
    }

    private Path getDocFixture() throws Exception {
        return Path.of(getClass().getResource("/test.doc").toURI());
    }

    private Path getDocxFixture() throws Exception {
        return Path.of(getClass().getResource("/test.docx").toURI());
    }

    private SeaTunnelRow findRowById(List<SeaTunnelRow> rows, String elementId) {
        return rows.stream()
                .filter(row -> elementId.equals(row.getField(0)))
                .findFirst()
                .orElse(null);
    }
}
