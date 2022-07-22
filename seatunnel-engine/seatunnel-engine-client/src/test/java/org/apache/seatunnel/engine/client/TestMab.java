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

package org.apache.seatunnel.engine.client;

import org.apache.seatunnel.core.base.config.ConfigBuilder;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.nio.file.Paths;
import java.util.List;

@RunWith(JUnit4.class)
public class TestMab {

    @Test
    public void testAbs() {
        Config seaTunnelJobConfig = new ConfigBuilder(Paths.get("/Users/gaojun/workspace/incubator-seatunnel/seatunnel-engine/seatunnel-engine-client/src/test/resources/fakesource_to_file.conf")).getConfig();
        List<? extends Config> sinkConfigs = seaTunnelJobConfig.getConfigList("sink");
        List<? extends Config> transformConfigs = seaTunnelJobConfig.getConfigList("transform");
        List<? extends Config> sourceConfigs = seaTunnelJobConfig.getConfigList("source");
        System.out.println(1);
    }
}
