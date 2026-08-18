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

import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.nio.file.Paths;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT;

class WordReadStrategyTest {

    @Test
    void testWordReadStrategy() throws Exception {
        URL resource = this.getClass().getResource("/test.docx");
        String path = Paths.get(resource.toURI()).toString();
        WordReadStrategy wordReadStrategy = new WordReadStrategy();
        wordReadStrategy.init(new LocalConf(FS_DEFAULT_NAME_DEFAULT));
        TempCollector tempCollector = new TempCollector();
        wordReadStrategy.read(path, "", tempCollector);

        Assertions.assertEquals(1, tempCollector.getRows().get(0).getField(0));
        Assertions.assertEquals("paragraph", tempCollector.getRows().get(0).getField(1));
        Assertions.assertEquals(
                "Sample Word Document with Image and Features",
                tempCollector.getRows().get(0).getField(2));
        Assertions.assertEquals("NORMAL", tempCollector.getRows().get(0).getField(3));
        Assertions.assertNull(tempCollector.getRows().get(0).getField(4));
        Assertions.assertNull(tempCollector.getRows().get(0).getField(5));
        Assertions.assertNull(tempCollector.getRows().get(0).getField(6));
        Assertions.assertEquals("000000", tempCollector.getRows().get(0).getField(7));
        Assertions.assertEquals("CENTER", tempCollector.getRows().get(0).getField(8));
        Assertions.assertNull(tempCollector.getRows().get(0).getField(9));

        Assertions.assertEquals(2, tempCollector.getRows().get(1).getField(0));
        Assertions.assertEquals("paragraph", tempCollector.getRows().get(1).getField(1));
        Assertions.assertEquals(
                "This is a sample sentence with italic, underline, strikethrough",
                tempCollector.getRows().get(1).getField(2));
        Assertions.assertEquals("BOLD_ITALIC", tempCollector.getRows().get(1).getField(3));
        Assertions.assertEquals("SINGLE", tempCollector.getRows().get(1).getField(4));
        Assertions.assertNull(tempCollector.getRows().get(1).getField(5));
        Assertions.assertNull(tempCollector.getRows().get(1).getField(6));
        Assertions.assertEquals("000000", tempCollector.getRows().get(1).getField(7));
        Assertions.assertEquals("LEFT", tempCollector.getRows().get(1).getField(8));
        Assertions.assertNull(tempCollector.getRows().get(1).getField(9));

        Assertions.assertEquals(4, tempCollector.getRows().get(3).getField(0));
        Assertions.assertEquals("paragraph", tempCollector.getRows().get(3).getField(1));
        Assertions.assertEquals(
                "For more information, visit here", tempCollector.getRows().get(3).getField(2));
        Assertions.assertEquals("NORMAL", tempCollector.getRows().get(3).getField(3));
        Assertions.assertNull(tempCollector.getRows().get(3).getField(4));
        Assertions.assertNull(tempCollector.getRows().get(3).getField(5));
        Assertions.assertNull(tempCollector.getRows().get(3).getField(6));
        Assertions.assertEquals("000000", tempCollector.getRows().get(3).getField(7));
        Assertions.assertEquals("LEFT", tempCollector.getRows().get(3).getField(8));
        Assertions.assertEquals(
                "http://www.google.com", tempCollector.getRows().get(3).getField(9));

        Assertions.assertEquals(9, tempCollector.getRows().get(8).getField(0));
        Assertions.assertEquals("table", tempCollector.getRows().get(8).getField(1));
        Assertions.assertEquals(
                "Row 1, Col 1 | Row 1, Col 2 | Row 1, Col 3\nRow 2, Col 1 | Row 2, Col 2 | Row 2, Col 3\nRow 3, Col 1 | Row 3, Col 2 | Row 3, Col 3",
                tempCollector.getRows().get(8).getField(2));
        Assertions.assertNull(tempCollector.getRows().get(8).getField(3));
        Assertions.assertNull(tempCollector.getRows().get(8).getField(4));
        Assertions.assertNull(tempCollector.getRows().get(8).getField(5));
        Assertions.assertNull(tempCollector.getRows().get(8).getField(6));
        Assertions.assertNull(tempCollector.getRows().get(8).getField(7));
        Assertions.assertNull(tempCollector.getRows().get(8).getField(8));
        Assertions.assertNull(tempCollector.getRows().get(8).getField(9));

        Assertions.assertEquals(16, tempCollector.getRows().get(15).getField(0));
        Assertions.assertEquals("paragraph", tempCollector.getRows().get(15).getField(1));
        Assertions.assertEquals(
                "Sample filler text line 4.", tempCollector.getRows().get(15).getField(2));
        Assertions.assertEquals("NORMAL", tempCollector.getRows().get(15).getField(3));
        Assertions.assertNull(tempCollector.getRows().get(15).getField(4));
        Assertions.assertEquals(16, tempCollector.getRows().get(15).getField(5));
        Assertions.assertEquals("DIN Alternate Bold", tempCollector.getRows().get(15).getField(6));
        Assertions.assertEquals("000000", tempCollector.getRows().get(15).getField(7));
        Assertions.assertEquals("LEFT", tempCollector.getRows().get(15).getField(8));
        Assertions.assertNull(tempCollector.getRows().get(15).getField(9));
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
