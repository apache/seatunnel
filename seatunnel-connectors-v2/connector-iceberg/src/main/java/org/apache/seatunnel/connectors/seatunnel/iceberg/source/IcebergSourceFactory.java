/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *     contributor license agreements.  See the NOTICE file distributed with
 *     this work for additional information regarding copyright ownership.
 *     The ASF licenses this file to You under the Apache License, Version 2.0
 *     (the "License"); you may not use this file except in compliance with
 *     the License.  You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.iceberg.source;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.catalog.schema.TableSchemaOptions;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;

import com.google.auto.service.AutoService;

import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.CommonConfig.KEY_CASE_SENSITIVE;
import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.CommonConfig.KEY_CATALOG_NAME;
import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.CommonConfig.KEY_CATALOG_TYPE;
import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.CommonConfig.KEY_NAMESPACE;
import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.CommonConfig.KEY_TABLE;
import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.CommonConfig.KEY_URI;
import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.CommonConfig.KEY_WAREHOUSE;
import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergCatalogType.HIVE;
import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.SourceConfig.KEY_END_SNAPSHOT_ID;
import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.SourceConfig.KEY_START_SNAPSHOT_ID;
import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.SourceConfig.KEY_START_SNAPSHOT_TIMESTAMP;
import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.SourceConfig.KEY_STREAM_SCAN_STRATEGY;
import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.SourceConfig.KEY_USE_SNAPSHOT_ID;
import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.SourceConfig.KEY_USE_SNAPSHOT_TIMESTAMP;

@AutoService(Factory.class)
public class IcebergSourceFactory implements TableSourceFactory {

    @Override
    public String factoryIdentifier() {
        return "Iceberg";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        KEY_CATALOG_NAME, KEY_CATALOG_TYPE, KEY_WAREHOUSE, KEY_NAMESPACE, KEY_TABLE)
                .conditional(KEY_CATALOG_TYPE, HIVE, KEY_URI)
                .optional(
                        TableSchemaOptions.SCHEMA,
                        KEY_CASE_SENSITIVE,
                        KEY_START_SNAPSHOT_TIMESTAMP,
                        KEY_START_SNAPSHOT_ID,
                        KEY_END_SNAPSHOT_ID,
                        KEY_USE_SNAPSHOT_ID,
                        KEY_USE_SNAPSHOT_TIMESTAMP,
                        KEY_STREAM_SCAN_STRATEGY)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return IcebergSource.class;
    }
}
