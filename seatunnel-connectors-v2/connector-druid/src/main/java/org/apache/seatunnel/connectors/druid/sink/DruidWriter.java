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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringJoiner;

public class DruidWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {

    private static final Logger LOG = LoggerFactory.getLogger(DruidWriter.class);

    private static final String DEFAULT_LINE_DELIMITER = "\n";
    private static final String DEFAULT_FIELD_DELIMITER = ",";
    private static final String TIMESTAMP_SPEC_COLUMN_NAME = "timestamp";
    private static final String DRUID_ENDPOINT = "/druid/indexer/v1/task";

    private int batchSize;
    private int currentBatchSize = 0;

    private final HttpURLConnection httpURLConnection;
    private final DataSchema dataSchema;
    // @Getter(AccessLevel.PACKAGE)
    private final long processTime;
    // @Getter(AccessLevel.PACKAGE)
    private final transient StringBuffer data;

    private final ObjectMapper mapper;
    private final String coordinatorUrl;
    private final String datasource;
    private final SeaTunnelRowType seaTunnelRowType;

    public DruidWriter(
            SeaTunnelRowType seaTunnelRowType,
            String coordinatorUrl,
            String datasource,
            int batchSize)
            throws IOException {
        this.seaTunnelRowType = seaTunnelRowType;
        this.coordinatorUrl = coordinatorUrl;
        this.datasource = datasource;
        this.batchSize = batchSize;
        this.mapper = provideDruidSerializer();
        this.httpURLConnection = provideHttpURLConnection(coordinatorUrl);
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
        final byte[] input = inputJSON.getBytes();
        try (final OutputStream os = httpURLConnection.getOutputStream()) {
            os.write(input, 0, input.length);
        }
        try (final BufferedReader br =
                new BufferedReader(
                        new InputStreamReader(
                                httpURLConnection.getInputStream(), StandardCharsets.UTF_8))) {
            final StringBuilder response = new StringBuilder();
            String responseLine;
            while ((responseLine = br.readLine()) != null) {
                response.append(responseLine.trim());
            }
            LOG.info("Druid write task has been sent, and the response is {}", response);
        }
    }

    @Override
    public void close() throws IOException {
        flush();
    }

    private HttpURLConnection provideHttpURLConnection(final String coordinatorURL)
            throws IOException {
        final URL url = new URL("http://" + coordinatorURL + DRUID_ENDPOINT);
        final HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setRequestMethod("POST");
        con.setRequestProperty("Content-Type", "application/json");
        con.setRequestProperty("Accept", "application/json, text/plain, */*");
        con.setDoOutput(true);
        return con;
    }

    private ObjectMapper provideDruidSerializer() {
        final ObjectMapper mapper = new ObjectMapper();
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
                case STRING:
                    dimensionSchemas.add(new StringDimensionSchema(columnName));
                    break;
                case FLOAT:
                    dimensionSchemas.add(new FloatDimensionSchema(columnName));
                    break;
                case DOUBLE:
                    dimensionSchemas.add(new DoubleDimensionSchema(columnName));
                    break;
                case INT:
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

    /**
     * Provide ioConfig that influences how data is read from a source system. Here we generate a
     * CSV file from writing data and use the Native batch Ingestion method
     * (https://druid.apache.org/docs/latest/ingestion/index.html#batch) to load from CSV file. More
     * details in https://druid.apache.org/docs/latest/ingestion/ingestion-spec.html#ioconfig.
     */
    @VisibleForTesting
    ParallelIndexIOConfig provideDruidIOConfig(final StringBuffer data) {
        final List<String> formatList = Arrays.asList(seaTunnelRowType.getFieldNames());
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
    @VisibleForTesting
    String provideInputJSONString(final ParallelIndexSupervisorTask indexTask)
            throws JsonProcessingException {
        String taskJSON = mapper.writeValueAsString(indexTask);
        final ObjectNode jsonObject = (ObjectNode) JsonSerializer.parse(taskJSON);
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
