/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.seatunnel.config.sql;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;

public class SqlConfigBuilderTest {

    @Test
    public void testSqlConfigBuild() {

        File file = new File("src/test/resources/sql-config.sql");

        Config config = SqlConfigBuilder.of(file.toPath());
        Config sourceConfig = config.getConfigList("source").get(0);
        Assertions.assertEquals("FakeSource", sourceConfig.getString("plugin_name"));
        Assertions.assertEquals("test1", sourceConfig.getString("plugin_output"));

        List<?> transformConfigs = config.getConfigList("transform");
        Assertions.assertEquals(2, transformConfigs.size());
        Config transformConf1 = (Config) transformConfigs.get(0);
        Assertions.assertEquals("test1", transformConf1.getString("plugin_input"));
        Assertions.assertEquals("test09", transformConf1.getString("plugin_output"));
        Config transformConf2 = (Config) transformConfigs.get(1);
        Assertions.assertEquals("test09", transformConf2.getString("plugin_input"));
        Assertions.assertEquals("test09__temp1", transformConf2.getString("plugin_output"));

        Config sinkConfig = config.getConfigList("sink").get(0);
        Assertions.assertEquals("jdbc", sinkConfig.getString("plugin_name"));
        Assertions.assertEquals("test09__temp1", sinkConfig.getString("plugin_input"));
    }
}
