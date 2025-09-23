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

package org.apache.seatunnel.connectors.seatunnel.iotdbv2.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.iotdbv2.config.IoTDBv2SinkOptions;
import org.apache.seatunnel.connectors.seatunnel.iotdbv2.constant.SinkConstants;
import org.apache.seatunnel.connectors.seatunnel.iotdbv2.exception.IotdbConnectorException;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AutoService(Factory.class)
public class IoTDBv2SinkFactory implements TableSinkFactory {
    @Override
    public String factoryIdentifier() {
        return "IoTDBv2";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        IoTDBv2SinkOptions.NODE_URLS,
                        IoTDBv2SinkOptions.USERNAME,
                        IoTDBv2SinkOptions.PASSWORD,
                        IoTDBv2SinkOptions.STORAGE_GROUP,
                        IoTDBv2SinkOptions.KEY_DEVICE)
                .optional(
                        IoTDBv2SinkOptions.SQL_DIALECT,
                        IoTDBv2SinkOptions.KEY_TIMESTAMP,
                        IoTDBv2SinkOptions.KEY_TAG_FIELDS,
                        IoTDBv2SinkOptions.KEY_ATTRIBUTE_FIELDS,
                        IoTDBv2SinkOptions.KEY_MEASUREMENT_FIELDS,
                        IoTDBv2SinkOptions.BATCH_SIZE,
                        IoTDBv2SinkOptions.MAX_RETRIES,
                        IoTDBv2SinkOptions.RETRY_BACKOFF_MULTIPLIER_MS,
                        IoTDBv2SinkOptions.MAX_RETRY_BACKOFF_MS,
                        IoTDBv2SinkOptions.DEFAULT_THRIFT_BUFFER_SIZE,
                        IoTDBv2SinkOptions.MAX_THRIFT_FRAME_SIZE,
                        IoTDBv2SinkOptions.ZONE_ID,
                        IoTDBv2SinkOptions.ENABLE_RPC_COMPRESSION,
                        IoTDBv2SinkOptions.CONNECTION_TIMEOUT_IN_MS)
                .build();
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        ReadonlyConfig conf = context.getOptions();
        String targetSqlDialect;
        if (conf.get(IoTDBv2SinkOptions.SQL_DIALECT) != null) {
            String sqlDialect = conf.get(IoTDBv2SinkOptions.SQL_DIALECT);
            if (SinkConstants.TABLE.equalsIgnoreCase(sqlDialect)) {
                targetSqlDialect = SinkConstants.TABLE;
            } else {
                if (SinkConstants.TREE.equalsIgnoreCase(sqlDialect)) {
                    targetSqlDialect = SinkConstants.TREE;
                } else {
                    throw new IotdbConnectorException(
                            CommonErrorCode.ILLEGAL_ARGUMENT, "Sql dialect not supported");
                }
            }
        } else {
            targetSqlDialect = SinkConstants.TREE;
        }
        return () ->
                new IoTDBv2Sink(context.getOptions(), context.getCatalogTable(), targetSqlDialect);
    }
}
