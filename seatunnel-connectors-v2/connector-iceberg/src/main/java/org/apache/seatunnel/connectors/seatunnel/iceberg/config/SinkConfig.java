/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.seatunnel.connectors.seatunnel.iceberg.config;

import lombok.Getter;
import lombok.ToString;
import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

@Getter
@ToString
public class SinkConfig extends CommonConfig {
    private static final long serialVersionUID = -196561967575264253L;

    public static final Option<Integer> BATCH_SIZE =
            Options.key("batch_size")
                    .intType()
                    .defaultValue(1)
                    .withDescription("batch size");

    private int batchSize;

    private int batchIntervalMs;

    private final int maxDataSize = 1;

    private final boolean partitionedFanoutEnabled = false;

    private final String fileFormat = "parquet";

    public SinkConfig(Config pluginConfig) {
        super(pluginConfig);
    }

    public static SinkConfig loadConfig(Config pluginConfig) {
        SinkConfig sinkConfig = new SinkConfig(pluginConfig);

        if (pluginConfig.hasPath(BATCH_SIZE.key())) {
            sinkConfig.batchSize = pluginConfig.getInt(BATCH_SIZE.key());
        }

        return sinkConfig;
    }
}
