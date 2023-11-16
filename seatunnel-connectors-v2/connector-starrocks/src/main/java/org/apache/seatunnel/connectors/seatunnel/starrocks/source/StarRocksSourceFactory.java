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

package org.apache.seatunnel.connectors.seatunnel.starrocks.source;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.catalog.schema.TableSchemaOptions;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.connectors.seatunnel.starrocks.config.SourceConfig;

import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class StarRocksSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return "StarRocks";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        SourceConfig.NODE_URLS,
                        SourceConfig.USERNAME,
                        SourceConfig.PASSWORD,
                        SourceConfig.DATABASE,
                        SourceConfig.TABLE,
                        TableSchemaOptions.SCHEMA)
                .optional(
                        SourceConfig.MAX_RETRIES,
                        SourceConfig.QUERY_TABLET_SIZE,
                        SourceConfig.SCAN_FILTER,
                        SourceConfig.SCAN_MEM_LIMIT,
                        SourceConfig.SCAN_QUERY_TIMEOUT_SEC,
                        SourceConfig.SCAN_KEEP_ALIVE_MIN,
                        SourceConfig.SCAN_BATCH_ROWS,
                        SourceConfig.SCAN_CONNECT_TIMEOUT)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return StarRocksSource.class;
    }
}
