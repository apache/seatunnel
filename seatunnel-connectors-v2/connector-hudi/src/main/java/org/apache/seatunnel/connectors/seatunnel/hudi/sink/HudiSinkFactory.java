/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.hudi.sink;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.sink.SinkCommonOptions;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;

import com.google.auto.service.AutoService;

import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiOptions.BATCH_INTERVAL_MS;
import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiOptions.CONF_FILES_PATH;
import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiOptions.INSERT_SHUFFLE_PARALLELISM;
import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiOptions.MAX_COMMITS_TO_KEEP;
import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiOptions.MIN_COMMITS_TO_KEEP;
import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiOptions.OP_TYPE;
import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiOptions.PARTITION_FIELDS;
import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiOptions.RECORD_KEY_FIELDS;
import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiOptions.TABLE_DFS_PATH;
import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiOptions.TABLE_NAME;
import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiOptions.TABLE_TYPE;
import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiOptions.UPSERT_SHUFFLE_PARALLELISM;

@AutoService(Factory.class)
public class HudiSinkFactory implements TableSinkFactory {
    @Override
    public String factoryIdentifier() {
        return "Hudi";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(TABLE_DFS_PATH, TABLE_NAME)
                .optional(
                        CONF_FILES_PATH,
                        RECORD_KEY_FIELDS,
                        PARTITION_FIELDS,
                        TABLE_TYPE,
                        OP_TYPE,
                        BATCH_INTERVAL_MS,
                        INSERT_SHUFFLE_PARALLELISM,
                        UPSERT_SHUFFLE_PARALLELISM,
                        MIN_COMMITS_TO_KEEP,
                        MAX_COMMITS_TO_KEEP,
                        SinkCommonOptions.MULTI_TABLE_SINK_REPLICA)
                .build();
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        CatalogTable catalogTable = context.getCatalogTable();
        return () -> new HudiSink(context.getOptions(), catalogTable);
    }
}
