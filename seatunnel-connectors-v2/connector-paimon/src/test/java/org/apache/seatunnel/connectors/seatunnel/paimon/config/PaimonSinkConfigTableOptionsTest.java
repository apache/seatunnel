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
import org.apache.seatunnel.api.options.SinkConnectorCommonOptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class PaimonSinkConfigTableOptionsTest {

    @Test
    void testTableOptionsMergedIntoWriteProps() {
        Map<String, Object> config = baseConfig();
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("file.format", "parquet");
        tableOptions.put("bucket", "4");
        config.put(SinkConnectorCommonOptions.TABLE_OPTIONS.key(), tableOptions);

        PaimonSinkConfig sinkConfig = new PaimonSinkConfig(ReadonlyConfig.fromMap(config));

        Assertions.assertEquals("parquet", sinkConfig.getWriteProps().get("file.format"));
        Assertions.assertEquals("4", sinkConfig.getWriteProps().get("bucket"));
    }

    @Test
    void testWritePropsWinsOnKeyConflict() {
        Map<String, Object> config = baseConfig();
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("bucket", "4");
        tableOptions.put("file.format", "orc");
        config.put(SinkConnectorCommonOptions.TABLE_OPTIONS.key(), tableOptions);

        Map<String, String> writeProps = new HashMap<>();
        writeProps.put("bucket", "8");
        config.put(PaimonSinkOptions.WRITE_PROPS.key(), writeProps);

        PaimonSinkConfig sinkConfig = new PaimonSinkConfig(ReadonlyConfig.fromMap(config));

        Assertions.assertEquals("8", sinkConfig.getWriteProps().get("bucket"));
        Assertions.assertEquals("orc", sinkConfig.getWriteProps().get("file.format"));
    }

    @Test
    void testAbsentTableOptionsKeepsWritePropsOnly() {
        Map<String, Object> config = baseConfig();
        Map<String, String> writeProps = new HashMap<>();
        writeProps.put("bucket", "2");
        config.put(PaimonSinkOptions.WRITE_PROPS.key(), writeProps);

        PaimonSinkConfig sinkConfig = new PaimonSinkConfig(ReadonlyConfig.fromMap(config));

        Assertions.assertEquals(1, sinkConfig.getWriteProps().size());
        Assertions.assertEquals("2", sinkConfig.getWriteProps().get("bucket"));
    }

    private static Map<String, Object> baseConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(PaimonSinkOptions.WAREHOUSE.key(), "file:///tmp/paimon");
        config.put(PaimonSinkOptions.DATABASE.key(), "db");
        config.put(PaimonSinkOptions.TABLE.key(), "t");
        return config;
    }
}
