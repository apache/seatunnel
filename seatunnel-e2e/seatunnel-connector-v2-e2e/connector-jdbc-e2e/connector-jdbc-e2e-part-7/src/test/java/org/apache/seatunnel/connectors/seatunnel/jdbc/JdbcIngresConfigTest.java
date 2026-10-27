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

package org.apache.seatunnel.connectors.seatunnel.jdbc;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigParseOptions;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigRenderOptions;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigSyntax;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValueFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;

class JdbcIngresConfigTest {

    @ParameterizedTest
    @ValueSource(ints = {1, 2})
    void testCompleteJobConfig(int id) {
        String password = "quote\" backslash\\ newline\n literal${value}";
        Config connectionConfig =
                ConfigFactory.empty()
                        .withValue(
                                "ingres_url",
                                ConfigValueFactory.fromAnyRef("jdbc:ingres://localhost:II7/testdb"))
                        .withValue("ingres_username", ConfigValueFactory.fromAnyRef("test_user"))
                        .withValue("ingres_password", ConfigValueFactory.fromAnyRef(password))
                        .withValue(
                                "ingres_table_path",
                                ConfigValueFactory.fromAnyRef("owner.test_table"));
        Config config =
                ConfigFactory.parseString(
                                JdbcIngresIT.createJobConfig(connectionConfig, id)
                                        .root()
                                        .render(ConfigRenderOptions.concise()),
                                ConfigParseOptions.defaults().setSyntax(ConfigSyntax.JSON))
                        .resolve();

        Assertions.assertEquals(1, config.getConfig("env").getInt("parallelism"));
        Assertions.assertEquals("BATCH", config.getConfig("env").getString("job.mode"));
        Assertions.assertEquals(1, config.getConfigList("source").size());
        Config source = config.getConfigList("source").get(0);
        Assertions.assertEquals("Jdbc", source.getString("plugin_name"));
        Assertions.assertEquals("com.ingres.jdbc.IngresDriver", source.getString("driver"));
        Assertions.assertEquals("jdbc:ingres://localhost:II7/testdb", source.getString("url"));
        Assertions.assertEquals("test_user", source.getString("username"));
        Assertions.assertEquals(password, source.getString("password"));
        Assertions.assertEquals("owner.test_table", source.getString("table_path"));
        Assertions.assertEquals("where \"id\" = " + id, source.getString("where_condition"));

        Assertions.assertEquals(1, config.getConfigList("sink").size());
        Config sink = config.getConfigList("sink").get(0);
        Assertions.assertEquals("Assert", sink.getString("plugin_name"));
        List<? extends Config> rowRules = sink.getConfig("rules").getConfigList("row_rules");
        Assertions.assertEquals("MIN_ROW", rowRules.get(0).getString("rule_type"));
        Assertions.assertEquals("MAX_ROW", rowRules.get(1).getString("rule_type"));
        rowRules.forEach(rule -> Assertions.assertEquals(1, rule.getInt("rule_value")));
        List<? extends Config> fields = sink.getConfig("rules").getConfigList("field_rules");
        Assertions.assertEquals("id", fields.get(0).getString("field_name"));
        fields.get(0)
                .getConfigList("field_value")
                .forEach(rule -> Assertions.assertEquals(id, rule.getInt("rule_value")));
        Assertions.assertEquals("Order", fields.get(1).getString("field_name"));
        Config stringRule = fields.get(1).getConfigList("field_value").get(0);
        Assertions.assertEquals(id == 1 ? "NOT_NULL" : "NULL", stringRule.getString("rule_type"));
        Assertions.assertEquals("quoted column", stringRule.getString("equals_to"));
        Assertions.assertEquals("amount", fields.get(2).getString("field_name"));
        Assertions.assertEquals("decimal(10,2)", fields.get(2).getString("field_type"));
        fields.get(2)
                .getConfigList("field_value")
                .forEach(rule -> Assertions.assertEquals(14.25, rule.getDouble("rule_value")));
    }
}
