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

package org.apache.seatunnel.connectors.seatunnel.hive.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.file.sink.commit.FileAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.file.sink.commit.FileCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.file.sink.state.FileSinkState;
import org.apache.seatunnel.connectors.seatunnel.hive.config.HiveConfig;
import org.apache.seatunnel.connectors.seatunnel.hive.config.HiveConstants;

import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class HiveSinkFactory
        implements TableSinkFactory<
                SeaTunnelRow, FileSinkState, FileCommitInfo, FileAggregatedCommitInfo> {

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(HiveConfig.TABLE_NAME)
                .required(HiveConfig.METASTORE_URI)
                .optional(HiveConfig.ABORT_DROP_PARTITION_METADATA)
                .optional(BaseSinkConfig.KERBEROS_PRINCIPAL)
                .optional(BaseSinkConfig.KERBEROS_KEYTAB_PATH)
                .optional(BaseSinkConfig.REMOTE_USER)
                .optional(HiveConfig.HADOOP_CONF)
                .optional(HiveConfig.HADOOP_CONF_PATH)
                .optional(BaseSinkConfig.PARQUET_AVRO_WRITE_TIMESTAMP_AS_INT96)
                .build();
    }

    @Override
    public TableSink<SeaTunnelRow, FileSinkState, FileCommitInfo, FileAggregatedCommitInfo>
            createSink(TableSinkFactoryContext context) {
        ReadonlyConfig readonlyConfig = context.getOptions();
        CatalogTable catalogTable = context.getCatalogTable();
        return () -> new HiveSink(readonlyConfig, catalogTable);
    }

    @Override
    public String factoryIdentifier() {
        return HiveConstants.CONNECTOR_NAME;
    }
}
