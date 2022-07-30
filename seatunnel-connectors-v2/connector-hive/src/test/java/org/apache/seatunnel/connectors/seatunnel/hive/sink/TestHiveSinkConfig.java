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

package org.apache.seatunnel.connectors.seatunnel.hive.sink;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigResolveOptions;

import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.File;
import java.util.List;

@RunWith(JUnit4.class)
public class TestHiveSinkConfig {

    @Test
    public void testCreateHiveSinkConfig() {
        String[] fieldNames = new String[]{"name", "age"};
        SeaTunnelDataType[] seaTunnelDataTypes = new SeaTunnelDataType[]{BasicType.STRING_TYPE, BasicType.INT_TYPE};
        SeaTunnelRowType seaTunnelRowTypeInfo = new SeaTunnelRowType(fieldNames, seaTunnelDataTypes);
        String configFile = "fakesource_to_hive.conf";
        String configFilePath = System.getProperty("user.dir") + "/src/test/resources/" + configFile;
        Config config = ConfigFactory
            .parseFile(new File(configFilePath))
            .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true))
            .resolveWith(ConfigFactory.systemProperties(),
                ConfigResolveOptions.defaults().setAllowUnresolved(true));
        List<? extends Config> sink = config.getConfigList("sink");
        HiveSinkConfig hiveSinkConfig = new HiveSinkConfig(sink.get(0), seaTunnelRowTypeInfo);
    }
}
