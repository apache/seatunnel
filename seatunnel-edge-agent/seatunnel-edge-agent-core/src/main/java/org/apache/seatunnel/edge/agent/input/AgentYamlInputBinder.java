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
import org.apache.seatunnel.edge.agent.connector.AgentInput;
import org.apache.seatunnel.edge.agent.connector.AgentInputFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * YAML-layer binding only: maps each {@link AgentYamlConfig.InputDefinition} to a connector {@link
 * AgentInput} via {@link AgentInputFactory}. Types {@code file}, {@code log}, and {@code event} are
 * supported when present in config after {@link AgentYamlConfig#validate(AgentYamlConfig)}.
 */
public final class AgentYamlInputBinder {

    private AgentYamlInputBinder() {}

    public static List<AgentInputBinding> bindAll(List<AgentYamlConfig.InputDefinition> defs) {
        List<AgentInputBinding> out = new ArrayList<>(defs.size());
        for (AgentYamlConfig.InputDefinition def : defs) {
            String type = def.getType().trim();
            List<Path> paths = AgentYamlInputPaths.toConnectorPaths(def);
            boolean logReadFromBeginning = Boolean.TRUE.equals(def.getReadFromBeginning());
            AgentInput input = AgentInputFactory.create(type, paths, logReadFromBeginning);
            out.add(new AgentInputBinding(def.getId(), input));
        }
        return out;
    }
}
