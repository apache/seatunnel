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

package org.apache.seatunnel.connectors.seatunnel.starrocks.sink;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.starrocks.config.SinkConfig;

import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class StarRocksSinkFactory implements TableSinkFactory {
    @Override
    public String factoryIdentifier() {
        return "StarRocks";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(SinkConfig.NODE_URLS, SinkConfig.USERNAME, SinkConfig.PASSWORD, SinkConfig.DATABASE, SinkConfig.TABLE)
                .optional(SinkConfig.LABEL_PREFIX, SinkConfig.BATCH_MAX_SIZE, SinkConfig.BATCH_MAX_BYTES,
                        SinkConfig.BATCH_INTERVAL_MS, SinkConfig.MAX_RETRIES, SinkConfig.MAX_RETRY_BACKOFF_MS,
                        SinkConfig.RETRY_BACKOFF_MULTIPLIER_MS, SinkConfig.STARROCKS_SINK_CONFIG_PREFIX)
                .build();
    }
}
