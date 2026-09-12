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

package org.apache.seatunnel.connectors.seatunnel.starrocks.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.connectors.seatunnel.starrocks.config.StarRocksSinkOptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

class StarRocksSaveModeUtilTableOptionsTest {

    @Test
    void validateAllowsDefaultTemplateWithTableOptions() {
        ReadonlyConfig config = ReadonlyConfig.fromMap(new HashMap<>());
        Map<String, String> tableOptions = Collections.singletonMap("replication_num", "3");

        Assertions.assertDoesNotThrow(
                () -> StarRocksSaveModeUtil.INSTANCE.validateTableOptions(config, tableOptions));
    }

    @Test
    void validateRejectsCustomTemplateWithTableOptions() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(
                StarRocksSinkOptions.SAVE_MODE_CREATE_TEMPLATE.key(),
                "CREATE TABLE `${database}`.`${table}` (${rowtype_fields})");
        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        Map<String, String> tableOptions = Collections.singletonMap("replication_num", "3");

        SeaTunnelRuntimeException exception =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () ->
                                StarRocksSaveModeUtil.INSTANCE.validateTableOptions(
                                        config, tableOptions));

        Assertions.assertTrue(exception.getMessage().contains("custom save_mode_create_template"));
    }

    @Test
    void applyMergesPropertiesIntoSql() {
        String sql =
                "CREATE TABLE t (id INT) ENGINE=OLAP PROPERTIES (\n"
                        + "    \"replication_num\" = \"1\"\n"
                        + ")";
        Map<String, String> tableOptions = Collections.singletonMap("replication_num", "3");

        String merged =
                StarRocksSaveModeUtil.INSTANCE.applyTableOptionsToCreateTableSql(sql, tableOptions);

        Assertions.assertTrue(merged.contains("\"replication_num\" = \"3\""));
    }
}
