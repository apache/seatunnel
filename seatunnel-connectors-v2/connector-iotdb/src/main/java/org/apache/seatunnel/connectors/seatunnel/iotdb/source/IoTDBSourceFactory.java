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

package org.apache.seatunnel.connectors.seatunnel.iotdb.source;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;

import com.google.auto.service.AutoService;

import static org.apache.seatunnel.connectors.seatunnel.iotdb.config.CommonConfig.NODE_URLS;
import static org.apache.seatunnel.connectors.seatunnel.iotdb.config.CommonConfig.PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.iotdb.config.CommonConfig.USERNAME;
import static org.apache.seatunnel.connectors.seatunnel.iotdb.config.SourceConfig.ENABLE_CACHE_LEADER;
import static org.apache.seatunnel.connectors.seatunnel.iotdb.config.SourceConfig.FETCH_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.iotdb.config.SourceConfig.LOWER_BOUND;
import static org.apache.seatunnel.connectors.seatunnel.iotdb.config.SourceConfig.NUM_PARTITIONS;
import static org.apache.seatunnel.connectors.seatunnel.iotdb.config.SourceConfig.SQL;
import static org.apache.seatunnel.connectors.seatunnel.iotdb.config.SourceConfig.THRIFT_DEFAULT_BUFFER_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.iotdb.config.SourceConfig.THRIFT_MAX_FRAME_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.iotdb.config.SourceConfig.UPPER_BOUND;
import static org.apache.seatunnel.connectors.seatunnel.iotdb.config.SourceConfig.VERSION;

@AutoService(Factory.class)
public class IoTDBSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return "IoTDB";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(NODE_URLS, USERNAME, PASSWORD, SQL, ConnectorCommonOptions.SCHEMA)
                .optional(
                        FETCH_SIZE,
                        THRIFT_DEFAULT_BUFFER_SIZE,
                        THRIFT_MAX_FRAME_SIZE,
                        ENABLE_CACHE_LEADER,
                        VERSION,
                        LOWER_BOUND,
                        UPPER_BOUND,
                        NUM_PARTITIONS)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return IoTDBSource.class;
    }
}
