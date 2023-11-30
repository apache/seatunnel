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

package org.apache.seatunnel.connectors.seatunnel.fake.config;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;

class MultipleTableFakeSourceConfigTest {

    @Test
    void getFakeConfigs() throws URISyntaxException {
        URL resource = MultipleTableFakeSourceConfigTest.class.getResource("/multiple_table.conf");
        Config config = ConfigFactory.parseFile(new File(Paths.get(resource.toURI()).toString()));
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromConfig(config.getConfig("FakeSource"));
        MultipleTableFakeSourceConfig multipleTableFakeSourceConfig =
                new MultipleTableFakeSourceConfig(readonlyConfig);
        Assertions.assertEquals(2, multipleTableFakeSourceConfig.getFakeConfigs().size());
    }
}
