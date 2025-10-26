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

package org.apache.seatunnel.connectors.seatunnel.iotdbv2.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.iotdbv2.config.IoTDBv2SourceOptions;
import org.apache.seatunnel.connectors.seatunnel.iotdbv2.constant.SourceConstants;
import org.apache.seatunnel.connectors.seatunnel.iotdbv2.exception.IotdbConnectorException;

import com.google.auto.service.AutoService;

import java.io.Serializable;

@AutoService(Factory.class)
public class IoTDBv2SourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return "IoTDBv2";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        IoTDBv2SourceOptions.NODE_URLS,
                        IoTDBv2SourceOptions.USERNAME,
                        IoTDBv2SourceOptions.PASSWORD,
                        IoTDBv2SourceOptions.SQL,
                        ConnectorCommonOptions.SCHEMA)
                .optional(
                        IoTDBv2SourceOptions.SQL_DIALECT,
                        IoTDBv2SourceOptions.DATABASE,
                        IoTDBv2SourceOptions.FETCH_SIZE,
                        IoTDBv2SourceOptions.DEFAULT_THRIFT_BUFFER_SIZE,
                        IoTDBv2SourceOptions.MAX_THRIFT_FRAME_SIZE,
                        IoTDBv2SourceOptions.ENABLE_CACHE_LEADER,
                        IoTDBv2SourceOptions.LOWER_BOUND,
                        IoTDBv2SourceOptions.UPPER_BOUND,
                        IoTDBv2SourceOptions.NUM_PARTITIONS)
                .build();
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        CatalogTable catalogTable = CatalogTableUtil.buildWithConfig(context.getOptions());
        ReadonlyConfig conf = context.getOptions();
        String targetSqlDialect;
        if (conf.get(IoTDBv2SourceOptions.SQL_DIALECT) != null) {
            String sqlDialect = conf.get(IoTDBv2SourceOptions.SQL_DIALECT);
            if (SourceConstants.TABLE.equalsIgnoreCase(sqlDialect)) {
                targetSqlDialect = SourceConstants.TABLE;
            } else {
                if (SourceConstants.TREE.equalsIgnoreCase(sqlDialect)) {
                    targetSqlDialect = SourceConstants.TREE;
                } else {
                    throw new IotdbConnectorException(
                            CommonErrorCode.ILLEGAL_ARGUMENT, "Sql dialect not supported");
                }
            }
        } else {
            targetSqlDialect = SourceConstants.TREE;
        }
        return () ->
                (SeaTunnelSource<T, SplitT, StateT>)
                        new IoTDBv2Source(catalogTable, context.getOptions(), targetSqlDialect);
    }

    @Override
    public Class<IoTDBv2Source> getSourceClass() {
        return IoTDBv2Source.class;
    }
}
