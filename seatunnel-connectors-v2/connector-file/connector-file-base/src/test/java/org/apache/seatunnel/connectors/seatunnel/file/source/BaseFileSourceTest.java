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

package org.apache.seatunnel.connectors.seatunnel.file.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT;

class BaseFileSourceTest {

    private static final String[] MARKDOWN_FIELD_NAMES = {
        "element_id",
        "element_type",
        "heading_level",
        "text",
        "page_number",
        "position_index",
        "parent_id",
        "child_ids"
    };
    private static final String[] MARKDOWN_RAG_METADATA_FIELD_NAMES = {
        "source_uri", "document_id", "chunk_id", "chunk_index", "content_hash"
    };

    @TempDir private Path tempDir;

    @Test
    void testMarkdownSourceDiscoversSchemaFromEmptyDirectory() {
        BaseFileSource source = new TestFileSource(createMarkdownConfig(false));

        CatalogTable catalogTable = source.getProducedCatalogTables().get(0);

        Assertions.assertArrayEquals(
                MARKDOWN_FIELD_NAMES, catalogTable.getSeaTunnelRowType().getFieldNames());
    }

    @Test
    void testMarkdownSourceDiscoversRagMetadataSchemaFromEmptyDirectory() {
        BaseFileSource source = new TestFileSource(createMarkdownConfig(true));

        CatalogTable catalogTable = source.getProducedCatalogTables().get(0);

        Assertions.assertArrayEquals(
                concat(MARKDOWN_FIELD_NAMES, MARKDOWN_RAG_METADATA_FIELD_NAMES),
                catalogTable.getSeaTunnelRowType().getFieldNames());
    }

    private ReadonlyConfig createMarkdownConfig(boolean ragMetadataEnabled) {
        Map<String, Object> map = new HashMap<>();
        map.put(FileBaseSourceOptions.FILE_PATH.key(), tempDir.toString());
        map.put(FileBaseSourceOptions.FILE_FORMAT_TYPE.key(), "markdown");
        map.put(FileBaseSourceOptions.MARKDOWN_RAG_METADATA_ENABLED.key(), ragMetadataEnabled);
        return ReadonlyConfig.fromMap(map);
    }

    private static String[] concat(String[] left, String[] right) {
        String[] result = new String[left.length + right.length];
        System.arraycopy(left, 0, result, 0, left.length);
        System.arraycopy(right, 0, result, left.length, right.length);
        return result;
    }

    private static class TestFileSource extends BaseFileSource {

        private TestFileSource(ReadonlyConfig pluginConfig) {
            super(pluginConfig);
        }

        @Override
        protected HadoopConf initHadoopConf() {
            return new LocalConf(FS_DEFAULT_NAME_DEFAULT);
        }

        @Override
        public String getPluginName() {
            return "TestFileSource";
        }
    }

    private static class LocalConf extends HadoopConf {

        private static final String HDFS_IMPL = "org.apache.hadoop.fs.RawLocalFileSystem";
        private static final String SCHEMA = "file";

        private LocalConf(String hdfsNameKey) {
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
