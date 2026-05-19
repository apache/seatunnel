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

import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT;

class MarkdownReadStrategyTest {

    private static final String[] DEFAULT_FIELD_NAMES = {
        "element_id",
        "element_type",
        "heading_level",
        "text",
        "page_number",
        "position_index",
        "parent_id",
        "child_ids"
    };
    private static final String[] RAG_FIELD_NAMES = {
        "source_uri", "document_id", "chunk_id", "chunk_index", "content_hash"
    };

    @TempDir private Path tempDir;

    @Test
    public void testReadMarkdown() throws Exception {
        URL resource = this.getClass().getResource("/test.md");
        String path = Paths.get(resource.toURI()).toString();
        AbstractReadStrategy markdownReadStrategy = createMarkdownReadStrategy();
        SeaTunnelRowType rowType = markdownReadStrategy.getSeaTunnelRowTypeInfo(path);
        TempCollector tempCollector = new TempCollector();
        markdownReadStrategy.read(path, "", tempCollector);

        Assertions.assertArrayEquals(DEFAULT_FIELD_NAMES, rowType.getFieldNames());
        Assertions.assertEquals(75, tempCollector.getRows().size());
        Assertions.assertEquals(
                DEFAULT_FIELD_NAMES.length, tempCollector.getRows().get(0).getArity());

        Assertions.assertEquals("Heading_1", tempCollector.getRows().get(0).getField(0));
        Assertions.assertEquals("Heading", tempCollector.getRows().get(0).getField(1));
        Assertions.assertEquals(1, tempCollector.getRows().get(0).getField(2));
        Assertions.assertEquals(
                "The Essential Guide to Groceries: Shopping, Storing, and Enjoying Food at Home",
                tempCollector.getRows().get(0).getField(3));
        Assertions.assertEquals(1, tempCollector.getRows().get(0).getField(4));
        Assertions.assertEquals(1, tempCollector.getRows().get(0).getField(5));
        Assertions.assertNull(tempCollector.getRows().get(0).getField(6));
        Assertions.assertNull(tempCollector.getRows().get(0).getField(7));

        Assertions.assertEquals("OrderedList_1", tempCollector.getRows().get(3).getField(0));
        Assertions.assertEquals("OrderedList", tempCollector.getRows().get(3).getField(1));
        Assertions.assertNull(tempCollector.getRows().get(3).getField(2));
        Assertions.assertEquals(
                "1. [Introduction](#introduction)\n"
                        + "2. [Grocery Categories](#grocery-categories)\n"
                        + "3. [Planning Your Grocery Trip](#planning-your-grocery-trip)\n"
                        + "4. [Shopping Tips for Savings](#shopping-tips-for-savings)\n"
                        + "5. [Storing and Organizing Groceries](#storing-and-organizing-groceries)\n"
                        + "6. [Healthy Choices](#healthy-choices)\n"
                        + "7. [Modern Grocery Trends](#modern-grocery-trends)\n"
                        + "8. [Comparison Table](#comparison-table)\n"
                        + "9. [Conclusion](#conclusion)\n",
                tempCollector.getRows().get(3).getField(3));
        Assertions.assertEquals(1, tempCollector.getRows().get(3).getField(4));
        Assertions.assertEquals(5, tempCollector.getRows().get(3).getField(5));
        Assertions.assertNull(tempCollector.getRows().get(3).getField(6));
        Assertions.assertEquals(
                "OrderedListItem_1,OrderedListItem_2,OrderedListItem_3,OrderedListItem_4,OrderedListItem_5,OrderedListItem_6,OrderedListItem_7,OrderedListItem_8,OrderedListItem_9",
                tempCollector.getRows().get(3).getField(7));

        Assertions.assertEquals("OrderedListItem_1", tempCollector.getRows().get(4).getField(0));
        Assertions.assertEquals("OrderedListItem", tempCollector.getRows().get(4).getField(1));
        Assertions.assertNull(tempCollector.getRows().get(4).getField(2));
        Assertions.assertEquals(
                "[Introduction](#introduction)", tempCollector.getRows().get(4).getField(3));
        Assertions.assertEquals(1, tempCollector.getRows().get(4).getField(4));
        Assertions.assertEquals(1, tempCollector.getRows().get(4).getField(5));
        Assertions.assertEquals("OrderedList_1", tempCollector.getRows().get(4).getField(6));
        Assertions.assertNull(tempCollector.getRows().get(4).getField(7));
    }

    @Test
    public void testReadMarkdownWithFileUri() throws Exception {
        Path markdownFile = tempDir.resolve("doc.md");
        Files.write(markdownFile, Arrays.asList("# Title"), StandardCharsets.UTF_8);

        AbstractReadStrategy markdownReadStrategy = createMarkdownReadStrategy();
        TempCollector tempCollector = new TempCollector();
        markdownReadStrategy.read(markdownFile.toUri().toString(), "", tempCollector);

        Assertions.assertEquals(1, tempCollector.getRows().size());
        Assertions.assertEquals("Title", tempCollector.getRows().get(0).getField(3));
    }

    @Test
    public void testReadMarkdownWithRagMetadata() throws Exception {
        URL resource = this.getClass().getResource("/test.md");
        String path = Paths.get(resource.toURI()).toString();
        AbstractReadStrategy markdownReadStrategy = createRagMetadataMarkdownReadStrategy();
        SeaTunnelRowType rowType = markdownReadStrategy.getSeaTunnelRowTypeInfo(path);
        TempCollector firstCollector = new TempCollector();
        markdownReadStrategy.read(path, "", firstCollector);

        Assertions.assertArrayEquals(
                concat(DEFAULT_FIELD_NAMES, RAG_FIELD_NAMES), rowType.getFieldNames());
        Assertions.assertEquals(75, firstCollector.getRows().size());
        Assertions.assertEquals(13, firstCollector.getRows().get(0).getArity());
        Assertions.assertEquals(path, firstCollector.getRows().get(0).getField(8));
        Assertions.assertTrue(
                String.valueOf(firstCollector.getRows().get(0).getField(9)).startsWith("doc_"));
        Assertions.assertTrue(
                String.valueOf(firstCollector.getRows().get(0).getField(10)).startsWith("chunk_"));
        Assertions.assertEquals(1, firstCollector.getRows().get(0).getField(11));
        Assertions.assertEquals(
                64, String.valueOf(firstCollector.getRows().get(0).getField(12)).length());

        AbstractReadStrategy secondReadStrategy = createRagMetadataMarkdownReadStrategy();
        TempCollector secondCollector = new TempCollector();
        secondReadStrategy.read(path, "", secondCollector);

        for (int fieldIndex = 8; fieldIndex < 13; fieldIndex++) {
            Assertions.assertEquals(
                    firstCollector.getRows().get(0).getField(fieldIndex),
                    secondCollector.getRows().get(0).getField(fieldIndex));
        }
    }

    @Test
    public void testReadMarkdownWithRagMetadataNormalizesFileUri() throws Exception {
        Path markdownFile = tempDir.resolve("doc.md");
        Files.write(markdownFile, Arrays.asList("# Title"), StandardCharsets.UTF_8);

        AbstractReadStrategy markdownReadStrategy = createRagMetadataMarkdownReadStrategy();
        TempCollector tempCollector = new TempCollector();
        markdownReadStrategy.read(markdownFile.toUri().toString(), "", tempCollector);

        AbstractReadStrategy expectedReadStrategy = createRagMetadataMarkdownReadStrategy();
        TempCollector expectedCollector = new TempCollector();
        expectedReadStrategy.read(markdownFile.toString(), "", expectedCollector);

        Assertions.assertEquals(
                markdownFile.toString(), tempCollector.getRows().get(0).getField(8));
        for (int fieldIndex = 8; fieldIndex < 11; fieldIndex++) {
            Assertions.assertEquals(
                    expectedCollector.getRows().get(0).getField(fieldIndex),
                    tempCollector.getRows().get(0).getField(fieldIndex));
        }
    }

    @Test
    public void testRagMetadataContentHashChangesWithText() throws Exception {
        Path markdownFile = tempDir.resolve("doc.md");
        Files.write(markdownFile, Arrays.asList("# First Title"), StandardCharsets.UTF_8);

        AbstractReadStrategy firstReadStrategy = createRagMetadataMarkdownReadStrategy();
        TempCollector firstCollector = new TempCollector();
        firstReadStrategy.read(markdownFile.toString(), "", firstCollector);
        Object firstDocumentId = firstCollector.getRows().get(0).getField(9);
        Object firstChunkIndex = firstCollector.getRows().get(0).getField(11);
        Object firstContentHash = firstCollector.getRows().get(0).getField(12);

        Files.write(markdownFile, Arrays.asList("# Second Title"), StandardCharsets.UTF_8);

        AbstractReadStrategy secondReadStrategy = createRagMetadataMarkdownReadStrategy();
        TempCollector secondCollector = new TempCollector();
        secondReadStrategy.read(markdownFile.toString(), "", secondCollector);

        Assertions.assertEquals(firstDocumentId, secondCollector.getRows().get(0).getField(9));
        Assertions.assertEquals(firstChunkIndex, secondCollector.getRows().get(0).getField(11));
        Assertions.assertNotEquals(firstContentHash, secondCollector.getRows().get(0).getField(12));
    }

    private static AbstractReadStrategy createMarkdownReadStrategy() {
        AbstractReadStrategy markdownReadStrategy = new MarkdownReadStrategy();
        markdownReadStrategy.init(new LocalConf(FS_DEFAULT_NAME_DEFAULT));
        return markdownReadStrategy;
    }

    private static AbstractReadStrategy createRagMetadataMarkdownReadStrategy() {
        AbstractReadStrategy markdownReadStrategy = createMarkdownReadStrategy();
        markdownReadStrategy.setPluginConfig(
                ConfigFactory.parseString("markdown_rag_metadata_enabled = true"));
        return markdownReadStrategy;
    }

    private static String[] concat(String[] left, String[] right) {
        String[] result = new String[left.length + right.length];
        System.arraycopy(left, 0, result, 0, left.length);
        System.arraycopy(right, 0, result, left.length, right.length);
        return result;
    }

    public static class LocalConf extends HadoopConf {
        private static final String HDFS_IMPL = "org.apache.hadoop.fs.LocalFileSystem";
        private static final String SCHEMA = "file";

        public LocalConf(String hdfsNameKey) {
            super(hdfsNameKey);
        }

        @Override
        public String getFsHdfsImpl() {
            return HDFS_IMPL;
        }

        @Override
        public String getSchema() {
            return SCHEMA;
        }
    }
}
