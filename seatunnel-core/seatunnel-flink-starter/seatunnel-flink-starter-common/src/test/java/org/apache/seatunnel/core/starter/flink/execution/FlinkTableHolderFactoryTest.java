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
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.core.starter.flink.utils.ConfigKeyName;

import org.junit.jupiter.api.Test;

import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FlinkTableHolderFactoryTest {

    @Test
    void getTableHolder() {
        Config config = ConfigFactory.parseMap(new HashMap<>());
        FlinkTableHolder holder = FlinkTableHolderFactory.getTableHolder(config, null);
        assertNotNull(holder);
        assertTrue(holder instanceof DefaultFlinkTableHolder);

        Config newConfig =
                ConfigFactory.parseMap(
                        (new HashMap<String, Object>() {
                            {
                                put(ConfigKeyName.FLINK_TABLE_DISABLE, Boolean.TRUE.toString());
                            }
                        }));
        FlinkTableHolder newHolder = FlinkTableHolderFactory.getTableHolder(newConfig, null);
        assertNotNull(newHolder);
        assertTrue(newHolder instanceof DataStreamTableHolder);
    }
}
