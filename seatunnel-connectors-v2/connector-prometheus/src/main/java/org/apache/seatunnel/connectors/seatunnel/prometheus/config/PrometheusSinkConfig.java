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
package org.apache.seatunnel.connectors.seatunnel.prometheus.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpConfig;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkArgument;

@Setter
@Getter
@ToString
public class PrometheusSinkConfig extends HttpConfig {

    private String keyTimestamp;

    private String keyValue;

    private String keyLabel;

    private int batchSize;

    private long flushInterval;

    public static PrometheusSinkConfig loadConfig(ReadonlyConfig pluginConfig) {
        PrometheusSinkConfig sinkConfig = new PrometheusSinkConfig();
        if (pluginConfig.getOptional(PrometheusSinkOptions.KEY_VALUE).isPresent()) {
            sinkConfig.setKeyValue(pluginConfig.get(PrometheusSinkOptions.KEY_VALUE));
        }
        if (pluginConfig.getOptional(PrometheusSinkOptions.KEY_LABEL).isPresent()) {
            sinkConfig.setKeyLabel(pluginConfig.get(PrometheusSinkOptions.KEY_LABEL));
        }
        if (pluginConfig.getOptional(PrometheusSinkOptions.KEY_TIMESTAMP).isPresent()) {
            sinkConfig.setKeyTimestamp(pluginConfig.get(PrometheusSinkOptions.KEY_TIMESTAMP));
        }
        if (pluginConfig.getOptional(PrometheusSinkOptions.BATCH_SIZE).isPresent()) {
            int batchSize = checkIntArgument(pluginConfig.get(PrometheusSinkOptions.BATCH_SIZE));
            sinkConfig.setBatchSize(batchSize);
        }
        if (pluginConfig.getOptional(PrometheusSinkOptions.FLUSH_INTERVAL).isPresent()) {
            long flushInterval = pluginConfig.get(PrometheusSinkOptions.FLUSH_INTERVAL);
            sinkConfig.setFlushInterval(flushInterval);
        }
        return sinkConfig;
    }

    private static int checkIntArgument(int args) {
        checkArgument(args > 0);
        return args;
    }
}
