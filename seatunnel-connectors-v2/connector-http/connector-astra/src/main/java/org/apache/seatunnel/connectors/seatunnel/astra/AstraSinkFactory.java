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

package org.apache.seatunnel.connectors.seatunnel.astra;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import com.google.auto.service.AutoService;

/**
 * Registers the Astra DB vector sink and maps SeaTunnel catalog fields to document inputs.
 * Connecting and mutation are deferred to the sink writer.
 */
@AutoService(Factory.class)
public final class AstraSinkFactory
        implements TableSinkFactory<SeaTunnelRow, AstraSink.State, Void, Void> {
    @Override
    public String factoryIdentifier() {
        return AstraSink.IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        AstraConfig.ENDPOINT,
                        AstraConfig.TOKEN,
                        AstraConfig.KEYSPACE,
                        AstraConfig.COLLECTION,
                        AstraConfig.EMBEDDING_MODE)
                .optional(
                        AstraConfig.MODE,
                        AstraConfig.NAMESPACE,
                        AstraConfig.STREAM,
                        AstraConfig.NAMESPACE_FIELD,
                        AstraConfig.STREAM_FIELD,
                        AstraConfig.PRIMARY_KEYS,
                        AstraConfig.TEXT_FIELDS,
                        AstraConfig.METADATA_FIELDS,
                        AstraConfig.FIELD_NAME_MAPPINGS,
                        AstraConfig.OMIT_RAW_TEXT,
                        AstraConfig.SPLITTER,
                        AstraConfig.SEPARATORS,
                        AstraConfig.KEEP_SEPARATOR,
                        AstraConfig.MARKDOWN_LEVEL,
                        AstraConfig.CODE_LANGUAGE,
                        AstraConfig.CHUNK_SIZE,
                        AstraConfig.CHUNK_OVERLAP,
                        AstraConfig.TOKENIZER_DATA_FILE,
                        AstraConfig.EMBEDDING_TOKEN,
                        AstraConfig.EMBEDDING_URL,
                        AstraConfig.EMBEDDING_MODEL,
                        AstraConfig.EMBEDDING_DEPLOYMENT,
                        AstraConfig.EMBEDDING_FIELD,
                        AstraConfig.EMBEDDING_DIMENSIONS,
                        AstraConfig.MAX_ROW_BYTES,
                        AstraConfig.MAX_BUFFER_BYTES,
                        AstraConfig.MAX_BODY_BYTES,
                        AstraConfig.MAX_CHUNKS_PER_ROW,
                        AstraConfig.MAX_DELETE_REQUESTS,
                        AstraConfig.CONNECT_TIMEOUT,
                        AstraConfig.READ_TIMEOUT,
                        AstraConfig.REQUEST_TIMEOUT,
                        AstraConfig.FLUSH_TIMEOUT,
                        AstraConfig.MAX_ATTEMPTS)
                .build();
    }

    @Override
    public TableSink<SeaTunnelRow, AstraSink.State, Void, Void> createSink(
            TableSinkFactoryContext context) {
        CatalogTable input = context.getCatalogTable();
        AstraConfig options = new AstraConfig(context.getOptions(), input.getTablePath());
        return () -> new AstraSink(options, input);
    }
}
