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

package org.apache.seatunnel.plugin.discovery;

import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.common.constants.PluginType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.util.Map;
import java.util.Objects;

@DisabledOnOs(OS.WINDOWS)
public class AbstractPluginDiscoveryTest {

    @Test
    public void testGetAllPlugins() {
        Common.setDeployMode(DeployMode.CLIENT);
        System.setProperty("SEATUNNEL_HOME", Objects.requireNonNull(AbstractPluginDiscoveryTest.class.getResource("/home")).getPath());
        Map<PluginIdentifier, String> sourcePlugins = AbstractPluginDiscovery.getAllSupportedPlugins(PluginType.SOURCE);
        Assertions.assertEquals(27, sourcePlugins.size());

        Map<PluginIdentifier, String> sinkPlugins = AbstractPluginDiscovery.getAllSupportedPlugins(PluginType.SINK);
        Assertions.assertEquals(30, sinkPlugins.size());

    }

}
