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

package org.apache.seatunnel.connectors.seatunnel.hbase.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.options.SinkConnectorCommonOptions;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.hbase.constant.HbaseIdentifier;

import com.google.auto.service.AutoService;

import static org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseConfig.DATA_SAVE_MODE;
import static org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseConfig.ENCODING;
import static org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseConfig.FAMILY_NAME;
import static org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseConfig.HBASE_EXTRA_CONFIG;
import static org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseConfig.NULL_MODE;
import static org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseConfig.ROWKEY_COLUMNS;
import static org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseConfig.ROWKEY_DELIMITER;
import static org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseConfig.SCHEMA_SAVE_MODE;
import static org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseConfig.TABLE;
import static org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseConfig.VERSION_COLUMN;
import static org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseConfig.WAL_WRITE;
import static org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseConfig.WRITE_BUFFER_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseConfig.ZOOKEEPER_QUORUM;

@AutoService(Factory.class)
public class HbaseSinkFactory implements TableSinkFactory {

    public static final String IDENTIFIER = "Hbase";

    @Override
    public String factoryIdentifier() {
        return HbaseIdentifier.IDENTIFIER_NAME;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        ZOOKEEPER_QUORUM,
                        TABLE,
                        ROWKEY_COLUMNS,
                        FAMILY_NAME,
                        SCHEMA_SAVE_MODE,
                        DATA_SAVE_MODE)
                .optional(
                        ROWKEY_DELIMITER,
                        VERSION_COLUMN,
                        NULL_MODE,
                        WAL_WRITE,
                        WRITE_BUFFER_SIZE,
                        ENCODING,
                        HBASE_EXTRA_CONFIG,
                        SinkConnectorCommonOptions.MULTI_TABLE_SINK_REPLICA)
                .build();
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        ReadonlyConfig readonlyConfig = context.getOptions();
        return () -> new HbaseSink(readonlyConfig, context.getCatalogTable());
    }
}
