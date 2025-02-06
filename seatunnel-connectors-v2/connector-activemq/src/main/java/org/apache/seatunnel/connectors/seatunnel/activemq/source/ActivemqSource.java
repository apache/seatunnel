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

package org.apache.seatunnel.connectors.seatunnel.activemq.source;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqConfig;
import org.apache.seatunnel.connectors.seatunnel.activemq.config.SchemaFormat;
import org.apache.seatunnel.connectors.seatunnel.activemq.split.ActivemqSplit;
import org.apache.seatunnel.connectors.seatunnel.activemq.split.ActivemqSplitEnumeratorState;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;
import org.apache.seatunnel.format.json.exception.SeaTunnelJsonFormatException;
import org.apache.seatunnel.format.text.TextDeserializationSchema;

import com.google.auto.service.AutoService;

import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqConfig.FIELD_DELIMITER;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqConfig.FORMAT;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqConfig.SCHEMA;

@AutoService(SeaTunnelSource.class)
public class ActivemqSource
        implements SeaTunnelSource<SeaTunnelRow, ActivemqSplit, ActivemqSplitEnumeratorState>,
                SupportParallelism {

    private DeserializationSchema<SeaTunnelRow> deserializationSchema;
    private JobContext jobContext;
    private ActivemqConfig activemqConfig;
    private SeaTunnelRowType typeInfo;
    private CatalogTable catalogTable;

    public static final String DEFAULT_FIELD_DELIMITER = ",";

    @Override
    public Boundedness getBoundedness() {
        return JobMode.BATCH.equals(jobContext.getJobMode())
                ? Boundedness.BOUNDED
                : Boundedness.UNBOUNDED;
    }

    @Override
    public String getPluginName() {
        return "ActiveMQ";
    }

    public ActivemqSource(ReadonlyConfig config, CatalogTable catalogTable) {
        this.activemqConfig = ActivemqConfig.of(config);
        this.catalogTable = catalogTable;
        this.typeInfo = catalogTable.getSeaTunnelRowType();
        setDeserialization(config);
    }

    @Override
    public SeaTunnelDataType getProducedType() {
        return deserializationSchema.getProducedType();
    }

    @Override
    public SourceReader<SeaTunnelRow, ActivemqSplit> createReader(
            SourceReader.Context readerContext) throws Exception {
        return new ActivemqSourceReader(deserializationSchema, readerContext, activemqConfig);
    }

    @Override
    public SourceSplitEnumerator<ActivemqSplit, ActivemqSplitEnumeratorState> createEnumerator(
            SourceSplitEnumerator.Context<ActivemqSplit> enumeratorContext) throws Exception {
        return new ActivemqSplitEnumerator();
    }

    @Override
    public SourceSplitEnumerator<ActivemqSplit, ActivemqSplitEnumeratorState> restoreEnumerator(
            SourceSplitEnumerator.Context<ActivemqSplit> enumeratorContext,
            ActivemqSplitEnumeratorState checkpointState)
            throws Exception {
        return new ActivemqSplitEnumerator();
    }

    @Override
    public void setJobContext(JobContext jobContext) {
        this.jobContext = jobContext;
    }

    private void setDeserialization(ReadonlyConfig config) {
        if (config.getOptional(SCHEMA).isPresent()) {
            SchemaFormat format = SchemaFormat.JSON;
            if (config.getOptional(FORMAT).isPresent()) {
                format = SchemaFormat.find(config.get(FORMAT));
            }
            switch (format) {
                case JSON:
                    deserializationSchema =
                            new JsonDeserializationSchema(catalogTable, false, false);
                    break;
                case TEXT:
                    String delimiter = DEFAULT_FIELD_DELIMITER;
                    if (config.getOptional(FIELD_DELIMITER).isPresent()) {
                        delimiter = config.get(FIELD_DELIMITER);
                    }
                    deserializationSchema =
                            TextDeserializationSchema.builder()
                                    .seaTunnelRowType(typeInfo)
                                    .delimiter(delimiter)
                                    .build();
                    break;
                default:
                    throw new SeaTunnelJsonFormatException(
                            CommonErrorCodeDeprecated.UNSUPPORTED_DATA_TYPE,
                            "Unsupported format: " + format);
            }
        } else {
            typeInfo = CatalogTableUtil.buildSimpleTextSchema();
            this.deserializationSchema =
                    TextDeserializationSchema.builder()
                            .seaTunnelRowType(typeInfo)
                            .delimiter(String.valueOf('\002'))
                            .build();
        }
    }
}
