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

package org.apache.seatunnel.edge.agent.starter.parse;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.edge.agent.connector.config.EdgeInputOptions;
import org.apache.seatunnel.edge.agent.transport.config.EdgeOutputOptions;

import java.util.HashMap;
import java.util.Map;

final class EdgeAgentTypeResolver {

    static String resolveInputType(ReadonlyConfig inputConfig) {
        return normalizeType(inputConfig, EdgeInputOptions.TYPE);
    }

    static String resolveOutputType(ReadonlyConfig outputConfig) {
        return normalizeType(outputConfig, EdgeOutputOptions.TYPE);
    }

    static ReadonlyConfig withInputType(ReadonlyConfig inputConfig, String inputType) {
        return withType(inputConfig, EdgeInputOptions.TYPE, inputType);
    }

    static ReadonlyConfig withOutputType(ReadonlyConfig outputConfig, String outputType) {
        return withType(outputConfig, EdgeOutputOptions.TYPE, outputType);
    }

    private static String normalizeType(ReadonlyConfig config, Option<String> typeOption) {
        String type = config.get(typeOption);
        if (type == null || type.trim().isEmpty()) {
            return typeOption.defaultValue();
        }
        return type.trim().toLowerCase();
    }

    private static ReadonlyConfig withType(
            ReadonlyConfig config, Option<String> typeOption, String type) {
        Map<String, Object> map = new HashMap<>(config.getSourceMap());
        map.put(typeOption.key(), type);
        return ReadonlyConfig.fromMap(map);
    }
}
