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

package org.apache.seatunnel.connectors.bigquery.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.options.SinkConnectorCommonOptions;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.bigquery.option.BigQuerySinkOptions;
import org.apache.seatunnel.connectors.bigquery.sink.committer.BigQueryCommitInfo;

import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class BigQuerySinkFactory implements TableSinkFactory {
    @Override
    public String factoryIdentifier() {
        return BigQuerySinkOptions.IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        BigQuerySinkOptions.PROJECT_ID,
                        BigQuerySinkOptions.DATASET_ID,
                        BigQuerySinkOptions.TABLE_ID)
                .optional(
                        BigQuerySinkOptions.SERVICE_ACCOUNT_KEY_PATH,
                        BigQuerySinkOptions.SERVICE_ACCOUNT_KEY_JSON,
                        BigQuerySinkOptions.WRITE_MODE,
                        BigQuerySinkOptions.SEQUENCE_NUMBER_COLUMN,
                        BigQuerySinkOptions.BATCH_SIZE,
                        BigQuerySinkOptions.EMULATOR_HOST,
                        SinkConnectorCommonOptions.MULTI_TABLE_SINK_REPLICA)
                .build();
    }

    @Override
    public TableSink<SeaTunnelRow, BigQuerySinkState, BigQueryCommitInfo, BigQueryCommitInfo>
            createSink(TableSinkFactoryContext context) {
        ReadonlyConfig config = context.getOptions();
        CatalogTable catalogTable = context.getCatalogTable();
        return () -> new BigQuerySink(config, catalogTable);
    }
}
