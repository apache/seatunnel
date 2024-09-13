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

package org.apache.seatunnel.plugin.discovery.seatunnel;

import com.google.common.collect.Lists;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class SeaTunnelSourcePluginDiscoveryTest {

    protected String originSeatunnelHome = null;

    protected static final String seatunnelHome =
            SeaTunnelSourcePluginDiscoveryTest.class.getResource("/duplicate").getPath();
    protected static final List<Path> pluginJars =
            Lists.newArrayList(
                    Paths.get(seatunnelHome, "connectors", "connector-http-jira.jar"),
                    Paths.get(seatunnelHome, "connectors", "connector-http.jar"),
                    Paths.get(seatunnelHome, "connectors", "connector-kafka.jar"),
                    Paths.get(seatunnelHome, "connectors", "connector-kafka-alcs.jar"),
                    Paths.get(seatunnelHome, "connectors", "connector-kafka-blcs.jar"));
}
