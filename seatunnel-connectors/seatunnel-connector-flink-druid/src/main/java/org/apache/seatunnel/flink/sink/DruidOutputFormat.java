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

package org.apache.seatunnel.flink.sink;

import static org.apache.flink.api.java.io.CsvInputFormat.DEFAULT_FIELD_DELIMITER;
import static org.apache.flink.api.java.io.CsvInputFormat.DEFAULT_LINE_DELIMITER;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.druid.data.input.MaxSizeSplitHintSpec;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.InlineInputSource;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexIOConfig;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexIngestionSpec;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexSupervisorTask;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexTuningConfig;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;

public class DruidOutputFormat extends RichOutputFormat<Row> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DruidOutputFormat.class);
    private static final long serialVersionUID = -7410857670269773005L;

    private static final String DEFAULT_TIMESTAMP_COLUMN = "timestamp";
    private static final String DEFAULT_TIMESTAMP_FORMAT = "auto";
    private static final DateTime DEFAULT_TIMESTAMP_MISSING_VALUE = null;

    private final transient StringBuffer data;
    private final String coordinatorURL;
    private final String datasource;
    private final String timestampColumn;
    private final String timestampFormat;
    private final DateTime timestampMissingValue;

    public DruidOutputFormat(String coordinatorURL,
                             String datasource,
                             String timestampColumn,
                             String timestampFormat,
                             String timestampMissingValue) {
        this.data = new StringBuffer();
        this.coordinatorURL = coordinatorURL;
        this.datasource = datasource;
        this.timestampColumn = timestampColumn == null ? DEFAULT_TIMESTAMP_COLUMN : timestampColumn;
        this.timestampFormat = timestampFormat == null ? DEFAULT_TIMESTAMP_FORMAT : timestampFormat;
        this.timestampMissingValue = timestampMissingValue == null ? DEFAULT_TIMESTAMP_MISSING_VALUE : DateTimes.of(timestampMissingValue);
    }

    @Override
    public void open(int taskNumber, int numTasks) {
    }

    @Override
    public void configure(Configuration parameters) {
    }

    @Override
    public void writeRecord(Row element) {
        int fieldIndex = element.getArity();
        for (int i = 0; i < fieldIndex; i++) {
            Object v = element.getField(i);
            if (i != 0) {
                this.data.append(DEFAULT_FIELD_DELIMITER);
            }
            if (v != null) {
                this.data.append(v);
            }
        }
        this.data.append(DEFAULT_LINE_DELIMITER);
    }

    @Override
    public void close() throws IOException {
        ParallelIndexIOConfig ioConfig = parallelIndexIOConfig();
        ParallelIndexTuningConfig tuningConfig = tuningConfig();
        ParallelIndexSupervisorTask indexTask = parallelIndexSupervisorTask(ioConfig, tuningConfig);
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.configure(MapperFeature.AUTO_DETECT_GETTERS, false);
        mapper.configure(MapperFeature.AUTO_DETECT_FIELDS, false);
        mapper.configure(MapperFeature.AUTO_DETECT_IS_GETTERS, false);
        mapper.configure(MapperFeature.AUTO_DETECT_SETTERS, false);
        mapper.configure(SerializationFeature.INDENT_OUTPUT, false);
        mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        String taskJSON = mapper.writeValueAsString(indexTask);
        JSONObject jsonObject = JSON.parseObject(taskJSON);
        jsonObject.remove("id");
        jsonObject.remove("groupId");
        jsonObject.remove("resource");
        JSONObject spec = jsonObject.getJSONObject("spec");
        spec.remove("tuningConfig");
        jsonObject.put("spec", spec);
        taskJSON = jsonObject.toJSONString();

        URL url = new URL(this.coordinatorURL + "druid/indexer/v1/task");
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setRequestMethod("POST");
        con.setRequestProperty("Content-Type", "application/json");
        con.setRequestProperty("Accept", "application/json, text/plain, */*");
        con.setDoOutput(true);
        try (OutputStream os = con.getOutputStream()) {
            byte[] input = taskJSON.getBytes(StandardCharsets.UTF_8);
            os.write(input, 0, input.length);
        }
        try (BufferedReader br = new BufferedReader(new InputStreamReader(con.getInputStream(), StandardCharsets.UTF_8))) {
            StringBuilder response = new StringBuilder();
            String responseLine;
            while ((responseLine = br.readLine()) != null) {
                response.append(responseLine.trim());
            }
            LOGGER.info("Druid write task has been sent, and the response is {}", response.toString());
        }
    }

    private ParallelIndexSupervisorTask parallelIndexSupervisorTask(ParallelIndexIOConfig ioConfig, ParallelIndexTuningConfig tuningConfig) {
        return new ParallelIndexSupervisorTask(
                null,
                null,
                null,
                new ParallelIndexIngestionSpec(
                        new DataSchema(
                                this.datasource,
                                new TimestampSpec(this.timestampColumn, this.timestampFormat, this.timestampMissingValue),
                                new DimensionsSpec(Collections.emptyList()),
                                null,
                                new UniformGranularitySpec(Granularities.HOUR, Granularities.MINUTE, false, null),
                                null
                        ),
                        ioConfig,
                        tuningConfig
                ),
                null
        );
    }

    private ParallelIndexIOConfig parallelIndexIOConfig() {
        return new ParallelIndexIOConfig(
                null,
                new InlineInputSource(this.data.toString()),
                new CsvInputFormat(
                        Arrays.asList("name", timestampColumn),
                        "|",
                        null,
                        false,
                        0
                ),
                false,
                null
        );
    }

    private ParallelIndexTuningConfig tuningConfig() {
        return new ParallelIndexTuningConfig(
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                new MaxSizeSplitHintSpec(null, 1),
                null,
                null,
                null,
                null,
                false,
                null,
                null,
                null,
                null,
                1,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null
        );
    }
}
