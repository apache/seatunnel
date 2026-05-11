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

package org.apache.seatunnel.edge.agent.input;

import org.apache.seatunnel.edge.agent.config.AgentYamlConfig;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

/**
 * Turns YAML {@link AgentYamlConfig.InputDefinition} path fields into the {@link List} of {@link
 * Path} values consumed by {@link org.apache.seatunnel.edge.agent.connector.AgentInputFactory}.
 */
final class AgentYamlInputPaths {

    private AgentYamlInputPaths() {}

    static List<Path> toConnectorPaths(AgentYamlConfig.InputDefinition def) {
        String type = def.getType().trim().toLowerCase(Locale.ROOT);
        switch (type) {
            case "file":
            case "event":
                List<Path> multi = new ArrayList<>(def.getPaths().size());
                for (String p : def.getPaths()) {
                    multi.add(Paths.get(p));
                }
                return multi;
            case "log":
                return Collections.singletonList(Paths.get(def.getPath()));
            default:
                throw new IllegalStateException(
                        "Unsupported input type after validation: " + def.getType());
        }
    }
}
