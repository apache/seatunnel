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

package org.apache.seatunnel.connectors.seatunnel.websocket.source;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitSource;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.websocket.config.WebSocketSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.websocket.config.WebSocketSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.websocket.exception.WebSocketConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.websocket.exception.WebSocketConnectorException;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;
import org.apache.seatunnel.format.text.TextDeserializationSchema;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Source reading the frames pushed by a {@code ws://} or {@code wss://} server. */
public class WebSocketSource extends AbstractSingleSplitSource<SeaTunnelRow> {

    private final WebSocketSourceConfig config;
    private final CatalogTable catalogTable;
    private final DeserializationSchema<SeaTunnelRow> deserializationSchema;
    private JobContext jobContext;

    public WebSocketSource(ReadonlyConfig readonlyConfig) {
        this.config = new WebSocketSourceConfig(readonlyConfig);
        Optional<Map<String, Object>> schemaOptions =
                readonlyConfig.getOptional(ConnectorCommonOptions.SCHEMA);
        if (!schemaOptions.isPresent()) {
            // without a schema every message is emitted as it was received, in a single column
            SeaTunnelRowType seaTunnelRowType =
                    new SeaTunnelRowType(
                            new String[] {"value"},
                            new SeaTunnelDataType<?>[] {BasicType.STRING_TYPE});
            this.catalogTable =
                    CatalogTableUtil.getCatalogTable(
                            WebSocketSourceOptions.identifier, seaTunnelRowType);
            this.deserializationSchema =
                    new WebSocketTextDeserializationSchema(catalogTable.getSeaTunnelRowType());
        } else {
            this.catalogTable = CatalogTableUtil.buildWithConfig(readonlyConfig);
            this.deserializationSchema = createDeserializationSchema();
        }
    }

    private DeserializationSchema<SeaTunnelRow> createDeserializationSchema() {
        switch (config.getFormat()) {
            case JSON:
                return new JsonDeserializationSchema(catalogTable, false, false);
            case TEXT:
                return TextDeserializationSchema.builder()
                        .seaTunnelRowType(catalogTable.getSeaTunnelRowType())
                        .delimiter(config.getFieldDelimiter())
                        .setCatalogTable(catalogTable)
                        .build();
            default:
                throw new WebSocketConnectorException(
                        WebSocketConnectorErrorCode.UNSUPPORTED_DATA_FORMAT,
                        String.format(
                                "Unsupported value [%s] of option [%s]",
                                config.getFormat(), WebSocketSourceOptions.FORMAT.key()));
        }
    }

    @Override
    public String getPluginName() {
        return WebSocketSourceOptions.identifier;
    }

    @Override
    public void setJobContext(JobContext jobContext) {
        this.jobContext = jobContext;
    }

    /**
     * A websocket server keeps pushing data, so the source is only bounded when the job itself runs
     * in batch mode. In that case the user must supply a stop condition, otherwise the batch job
     * would never finish.
     */
    @Override
    public Boundedness getBoundedness() {
        if (!JobMode.BATCH.equals(jobContext.getJobMode())) {
            return Boundedness.UNBOUNDED;
        }
        if (config.getMaxRecords() <= 0 && config.getReadTimeoutMs() <= 0) {
            throw new WebSocketConnectorException(
                    WebSocketConnectorErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "Running the WebSocket source in batch mode requires a stop condition, "
                                    + "please configure option [%s] or [%s]",
                            WebSocketSourceOptions.MAX_RECORDS.key(),
                            WebSocketSourceOptions.READ_TIMEOUT_MS.key()));
        }
        return Boundedness.BOUNDED;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return Collections.singletonList(catalogTable);
    }

    @Override
    public AbstractSingleSplitReader<SeaTunnelRow> createReader(
            SingleSplitReaderContext readerContext) {
        return new WebSocketSourceReader(this.config, readerContext, this.deserializationSchema);
    }
}
