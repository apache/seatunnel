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

package org.apache.seatunnel.core.starter.flink.execution;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.core.starter.utils.ConfigBuilder;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;

class FlinkRuntimeEnvironmentTest {

    @Test
    void prepare() {
        //

        Map<String, Object> json =
                JsonUtils.parseObject(
                        "{\n"
                                + "  \"env\": {\n"
                                + "    \"job.mode\": \"BATCH\",\n"
                                + "    \"parallelism\": 1\n"
                                + "  },\n"
                                + "  \"source\": [\n"
                                + "    {\n"
                                + "      \"plugin_name\": \"FakeSource\",\n"
                                + "      \"result_table_name\": \"fake\",\n"
                                + "      \"row.num\": 100,\n"
                                + "      \"schema\": {\n"
                                + "        \"fields\": {\n"
                                + "          \"card\": \"int\"\n"
                                + "        }\n"
                                + "      }\n"
                                + "    }\n"
                                + "  ],\n"
                                + "  \"sink\": [\n"
                                + "    {\n"
                                + "      \"plugin_name\": \"Console\"\n"
                                + "    }\n"
                                + "  ]\n"
                                + "}",
                        Map.class);
        Config config = ConfigBuilder.of(json);
        // assert flink table module is required to load
        assertThrows(
                org.apache.flink.table.api.TableException.class,
                () -> FlinkRuntimeEnvironment.getInstance(config));

        Map<String, Object> disableFlinkTableJson =
                JsonUtils.parseObject(
                        "{\n"
                                + "  \"env\": {\n"
                                + "    \"job.mode\": \"BATCH\",\n"
                                + "    \"parallelism\": 1,\n"
                                + "    \"flink.table.disable\": \"true\"\n"
                                + "  },\n"
                                + "  \"source\": [\n"
                                + "    {\n"
                                + "      \"plugin_name\": \"FakeSource\",\n"
                                + "      \"result_table_name\": \"fake\",\n"
                                + "      \"row.num\": 100,\n"
                                + "      \"schema\": {\n"
                                + "        \"fields\": {\n"
                                + "          \"name\": \"string\",\n"
                                + "          \"gender\": \"boolean\",\n"
                                + "          \"card\": \"int\"\n"
                                + "        }\n"
                                + "      }\n"
                                + "    }\n"
                                + "  ],\n"
                                + "  \"sink\": [\n"
                                + "    {\n"
                                + "      \"plugin_name\": \"Console\",\n"
                                + "      \"source_table_name\": \"fake\"\n"
                                + "    }\n"
                                + "  ]\n"
                                + "}",
                        Map.class);
        Config disableConfig = ConfigBuilder.of(disableFlinkTableJson);
        FlinkRuntimeEnvironment env = FlinkRuntimeEnvironment.getInstance(disableConfig);
        Assertions.assertNull(env.getStreamTableEnvironment());
    }
}
