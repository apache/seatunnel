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

package org.apache.seatunnel.connectors.seatunnel.rabbitmq.config;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.common.config.CheckConfigUtil;

import lombok.Getter;
import lombok.Setter;

import java.util.HashMap;
import java.util.Map;

@Setter
@Getter
public class RabbitmqSinkOptions extends RabbitmqBaseOptions {
    private final Map<String, Object> sinkOptionProps = new HashMap<>();

    void parseSinkOptionProperties(Config pluginConfig) {
        if (CheckConfigUtil.isValidParam(pluginConfig, RABBITMQ_CONFIG.key())) {
            pluginConfig
                    .getObject(RABBITMQ_CONFIG.key())
                    .forEach(
                            (key, value) -> {
                                final String configKey = key.toLowerCase();
                                sinkOptionProps.put(configKey, value.unwrapped());
                            });
        }
    }
}
