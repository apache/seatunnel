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

package org.apache.seatunnel.connectors.seatunnel.iotdb.sink;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.iotdb.config.IoTDBSinkOptions;

import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class IoTDBSinkFactory implements TableSinkFactory {
    @Override
    public String factoryIdentifier() {
        return "IoTDB";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        IoTDBSinkOptions.NODE_URLS,
                        IoTDBSinkOptions.USERNAME,
                        IoTDBSinkOptions.PASSWORD,
                        IoTDBSinkOptions.KEY_DEVICE)
                .optional(
                        IoTDBSinkOptions.KEY_TIMESTAMP,
                        IoTDBSinkOptions.KEY_MEASUREMENT_FIELDS,
                        IoTDBSinkOptions.STORAGE_GROUP,
                        IoTDBSinkOptions.BATCH_SIZE,
                        IoTDBSinkOptions.MAX_RETRIES,
                        IoTDBSinkOptions.RETRY_BACKOFF_MULTIPLIER_MS,
                        IoTDBSinkOptions.MAX_RETRY_BACKOFF_MS,
                        IoTDBSinkOptions.DEFAULT_THRIFT_BUFFER_SIZE,
                        IoTDBSinkOptions.MAX_THRIFT_FRAME_SIZE,
                        IoTDBSinkOptions.ZONE_ID,
                        IoTDBSinkOptions.ENABLE_RPC_COMPRESSION,
                        IoTDBSinkOptions.CONNECTION_TIMEOUT_IN_MS)
                .build();
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        return () -> new IoTDBSink(context.getOptions(), context.getCatalogTable());
    }
}
