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

package org.apache.seatunnel.edge.agent.connector.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class FileCollectConfigTest {

    @Test
    void appliesDefaults() {
        Map<String, Object> map = new HashMap<>();
        map.put(FileCollectOptions.ID.key(), "input-1");
        map.put(FileCollectOptions.PATHS.key(), Collections.singletonList("/tmp/a.log"));

        FileCollectConfig config = FileCollectConfig.from(ReadonlyConfig.fromMap(map));
        Assertions.assertEquals("UTF-8", config.getEncoding());
        Assertions.assertEquals(5000L, config.getGlobScanIntervalMs());
        Assertions.assertEquals("skip", config.getOnError());
    }

    @Test
    void rejectsEmptyPaths() {
        Map<String, Object> map = new HashMap<>();
        map.put(FileCollectOptions.ID.key(), "input-1");
        map.put(FileCollectOptions.PATHS.key(), Collections.emptyList());

        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> FileCollectConfig.from(ReadonlyConfig.fromMap(map)));
    }

    @Test
    void readsOutputFormatType() {
        Map<String, Object> map = new HashMap<>();
        map.put(FileCollectOptions.ID.key(), "input-1");
        map.put(FileCollectOptions.PATHS.key(), Collections.singletonList("/tmp/a.log"));
        map.put(FileCollectOptions.OUTPUT_FORMAT_TYPE.key(), "json");

        FileCollectConfig config = FileCollectConfig.from(ReadonlyConfig.fromMap(map));
        Assertions.assertTrue(config.isJsonOutput());
    }

    @Test
    void resolvesCharsetFromEncoding() {
        Map<String, Object> map = new HashMap<>();
        map.put(FileCollectOptions.ID.key(), "input-1");
        map.put(FileCollectOptions.PATHS.key(), Collections.singletonList("/tmp/a.log"));
        map.put(FileCollectOptions.ENCODING.key(), "GBK");

        FileCollectConfig config = FileCollectConfig.from(ReadonlyConfig.fromMap(map));
        Assertions.assertEquals("GBK", config.getEncoding());
        Assertions.assertEquals("GBK", config.getCharset().name());
    }

    @Test
    void rejectsUnsupportedEncoding() {
        Map<String, Object> map = new HashMap<>();
        map.put(FileCollectOptions.ID.key(), "input-1");
        map.put(FileCollectOptions.PATHS.key(), Collections.singletonList("/tmp/a.log"));
        map.put(FileCollectOptions.ENCODING.key(), "NOT_A_REAL_CHARSET_XYZ");

        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> FileCollectConfig.from(ReadonlyConfig.fromMap(map)));
    }

    @Test
    void rejectsInvalidOnError() {
        Map<String, Object> map = new HashMap<>();
        map.put(FileCollectOptions.ID.key(), "input-1");
        map.put(FileCollectOptions.PATHS.key(), Collections.singletonList("/tmp/a.log"));
        map.put(FileCollectOptions.ON_ERROR.key(), "ignore");

        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> FileCollectConfig.from(ReadonlyConfig.fromMap(map)));
    }
}
