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

package org.apache.seatunnel.connectors.seatunnel.hudi.sink;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.sink.SinkCommonOptions;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;

import com.google.auto.service.AutoService;

import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiOptions.AUTO_COMMIT;
import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiOptions.BATCH_INTERVAL_MS;
import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiOptions.BATCH_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiOptions.CONF_FILES_PATH;
import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiOptions.INSERT_SHUFFLE_PARALLELISM;
import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiOptions.MAX_COMMITS_TO_KEEP;
import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiOptions.MIN_COMMITS_TO_KEEP;
import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiOptions.OP_TYPE;
import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiOptions.TABLE_LIST;
import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiOptions.UPSERT_SHUFFLE_PARALLELISM;
import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiTableOptions.INDEX_CLASS_NAME;
import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiTableOptions.INDEX_TYPE;
import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiTableOptions.PARTITION_FIELDS;
import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiTableOptions.RECORD_BYTE_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiTableOptions.RECORD_KEY_FIELDS;
import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiTableOptions.TABLE_DFS_PATH;
import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiTableOptions.TABLE_NAME;
import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiTableOptions.TABLE_TYPE;

@AutoService(Factory.class)
public class HudiSinkFactory implements TableSinkFactory {
    @Override
    public String factoryIdentifier() {
        return "Hudi";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .optional(
                        TABLE_NAME,
                        TABLE_DFS_PATH,
                        TABLE_TYPE,
                        RECORD_KEY_FIELDS,
                        PARTITION_FIELDS,
                        INDEX_TYPE,
                        INDEX_CLASS_NAME,
                        RECORD_BYTE_SIZE,
                        TABLE_LIST,
                        CONF_FILES_PATH,
                        OP_TYPE,
                        BATCH_SIZE,
                        BATCH_INTERVAL_MS,
                        INSERT_SHUFFLE_PARALLELISM,
                        UPSERT_SHUFFLE_PARALLELISM,
                        MIN_COMMITS_TO_KEEP,
                        MAX_COMMITS_TO_KEEP,
                        AUTO_COMMIT,
                        SinkCommonOptions.MULTI_TABLE_SINK_REPLICA)
                .build();
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        return () -> new HudiSink(context.getOptions(), context.getCatalogTable());
    }
}
