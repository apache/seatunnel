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

package org.apache.seatunnel.connectors.druid.sink;

import org.apache.seatunnel.shade.com.google.common.annotations.VisibleForTesting;

import org.apache.seatunnel.api.sink.SupportMultiTableSinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.druid.exception.DruidConnectorException;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;

import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.DoubleDimensionSchema;
import org.apache.druid.data.input.impl.FloatDimensionSchema;
import org.apache.druid.data.input.impl.InlineInputSource;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexIOConfig;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexIngestionSpec;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexSupervisorTask;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.joda.JodaModule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Collectors;

public class DruidWriter extends AbstractSinkWriter<SeaTunnelRow, Void>
        implements SupportMultiTableSinkWriter<Void> {

    private static final Logger LOG = LoggerFactory.getLogger(DruidWriter.class);

    private static final String DEFAULT_LINE_DELIMITER = "\n";
    private static final String DEFAULT_FIELD_DELIMITER = ",";
    private static final String TIMESTAMP_SPEC_COLUMN_NAME = "timestamp";
    private static final String DRUID_ENDPOINT = "/druid/indexer/v1/task";

    private int batchSize;
    private int currentBatchSize = 0;

    private final DataSchema dataSchema;

    private final long processTime;
    private final transient StringBuffer data;

    private final CloseableHttpClient httpClient;
    private final ObjectMapper mapper;
    private final String coordinatorUrl;
    private final String datasource;
    private final SeaTunnelRowType seaTunnelRowType;

    public DruidWriter(
            SeaTunnelRowType seaTunnelRowType,
            String coordinatorUrl,
            String datasource,
            int batchSize) {
        this.seaTunnelRowType = seaTunnelRowType;
        this.coordinatorUrl = coordinatorUrl;
        this.datasource = datasource;
        this.batchSize = batchSize;
        this.mapper = provideDruidSerializer();
        this.httpClient = HttpClients.createDefault();
        this.dataSchema = provideDruidDataSchema();
        this.processTime = System.currentTimeMillis();
        this.data = new StringBuffer();
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        final StringJoiner joiner = new StringJoiner(DEFAULT_FIELD_DELIMITER, "", "");
        for (int i = 0; i < element.getArity(); i++) {
            final Object v = element.getField(i);
            if (v != null) {
                joiner.add(v.toString());
            }
        }
        // timestamp column is a required field to add in Druid.
        // See https://druid.apache.org/docs/24.0.0/ingestion/data-model.html#primary-timestamp
        joiner.add(String.valueOf(processTime));
        data.append(joiner);
        data.append(DEFAULT_LINE_DELIMITER);
        currentBatchSize++;
        if (currentBatchSize >= batchSize) {
            flush();
            currentBatchSize = 0;
        }
    }

    public void flush() throws IOException {
        final ParallelIndexIOConfig ioConfig = provideDruidIOConfig(data);
        final ParallelIndexSupervisorTask indexTask = provideIndexTask(ioConfig);
        final String inputJSON = provideInputJSONString(indexTask);
        String uri = new String("http://" + this.coordinatorUrl + DRUID_ENDPOINT);
        HttpPost post = new HttpPost(uri);
        post.setHeader("Content-Type", "application/json");
        post.setHeader("Accept", "application/json, text/plain, */*");
        post.setEntity(new StringEntity(inputJSON));

        try (CloseableHttpResponse response = httpClient.execute(post)) {
            String responseBody =
                    response.getEntity() != null ? response.getEntity().toString() : "";
            LOG.info("Druid write task has been sent, and the response is {}", responseBody);
        }
    }

    @Override
    public void close() throws IOException {
        flush();
        if (httpClient != null) {
            httpClient.close();
        }
    }

    private ObjectMapper provideDruidSerializer() {
        final ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JodaModule());
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.configure(MapperFeature.AUTO_DETECT_GETTERS, false);
        mapper.configure(MapperFeature.AUTO_DETECT_FIELDS, false);
        mapper.configure(MapperFeature.AUTO_DETECT_IS_GETTERS, false);
        mapper.configure(MapperFeature.AUTO_DETECT_SETTERS, false);
        mapper.configure(SerializationFeature.INDENT_OUTPUT, false);
        mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        return mapper;
    }

    /**
     * One necessary information to provide is DimensionSchema list, which states data type of
     * columns. More details in https://druid.apache.org/docs/latest/ingestion/ingestion-spec.html
     */
    private DataSchema provideDruidDataSchema() {
        final List<DimensionSchema> dimensionSchemas = transformToDimensionSchema();
        return new DataSchema(
                datasource,
                new TimestampSpec(TIMESTAMP_SPEC_COLUMN_NAME, "auto", null),
                new DimensionsSpec(dimensionSchemas),
                null,
                new UniformGranularitySpec(Granularities.HOUR, Granularities.MINUTE, false, null),
                null);
    }

    private List<DimensionSchema> transformToDimensionSchema() {
        List<DimensionSchema> dimensionSchemas = new ArrayList<>();
        String[] fieldNames = seaTunnelRowType.getFieldNames();
        SeaTunnelDataType<?>[] fieldTypes = seaTunnelRowType.getFieldTypes();
        for (int i = 0; i < fieldNames.length; i++) {
            String columnName = fieldNames[i];
            switch (fieldTypes[i].getSqlType()) {
                case BOOLEAN:
                case TIMESTAMP:
                case STRING:
                    dimensionSchemas.add(new StringDimensionSchema(columnName));
                    break;
                case FLOAT:
                    dimensionSchemas.add(new FloatDimensionSchema(columnName));
                    break;
                case DECIMAL:
                case DOUBLE:
                    dimensionSchemas.add(new DoubleDimensionSchema(columnName));
                    break;
                case TINYINT:
                case SMALLINT:
                case INT:
                case BIGINT:
                    dimensionSchemas.add(new LongDimensionSchema(columnName));
                    break;
                default:
                    throw new DruidConnectorException(
                            CommonErrorCodeDeprecated.UNSUPPORTED_DATA_TYPE,
                            "Unsupported data type " + seaTunnelRowType.getFieldType(i));
            }
        }
        return dimensionSchemas;
    }

    ParallelIndexIOConfig provideDruidIOConfig(final StringBuffer data) {
        List<String> formatList =
                Arrays.stream(seaTunnelRowType.getFieldNames()).collect(Collectors.toList());
        formatList.add(TIMESTAMP_SPEC_COLUMN_NAME);
        return new ParallelIndexIOConfig(
                null,
                new InlineInputSource(data.toString()),
                new CsvInputFormat(formatList, DEFAULT_LINE_DELIMITER, null, false, 0),
                false,
                null);
    }

    /**
     * Provide ParallelIndexSupervisorTask that can run multiple indexing tasks concurrently. See
     * more information in https://druid.apache.org/docs/latest/ingestion/native-batch.html
     */
    @VisibleForTesting
    ParallelIndexSupervisorTask provideIndexTask(final ParallelIndexIOConfig ioConfig) {
        return new ParallelIndexSupervisorTask(
                null, null, null, new ParallelIndexIngestionSpec(dataSchema, ioConfig, null), null);
    }

    /**
     * Provide JSON to be sent via HTTP request. Please see payload example in
     * https://druid.apache.org/docs/latest/ingestion/ingestion-spec.html
     */
    String provideInputJSONString(final ParallelIndexSupervisorTask indexTask)
            throws JsonProcessingException {
        String taskJSON = mapper.writeValueAsString(indexTask);
        final ObjectNode jsonObject = (ObjectNode) mapper.readTree(taskJSON);
        jsonObject.remove("id");
        jsonObject.remove("groupId");
        jsonObject.remove("resource");

        final ObjectNode spec = (ObjectNode) jsonObject.get("spec");
        spec.remove("tuningConfig");
        jsonObject.put("spec", spec);
        taskJSON = jsonObject.toString();
        return taskJSON;
    }
}
