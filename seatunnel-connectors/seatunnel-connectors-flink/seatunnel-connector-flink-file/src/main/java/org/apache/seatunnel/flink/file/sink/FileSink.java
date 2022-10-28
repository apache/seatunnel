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

package org.apache.seatunnel.flink.file.sink;

import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.config.TypesafeConfigUtils;
import org.apache.seatunnel.common.utils.VariablesSubstitute;
import org.apache.seatunnel.flink.BaseFlinkSink;
import org.apache.seatunnel.flink.FlinkEnvironment;
import org.apache.seatunnel.flink.batch.FlinkBatchSink;
import org.apache.seatunnel.flink.enums.FormatType;
import org.apache.seatunnel.flink.stream.FlinkStreamSink;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.types.Row;

import java.util.concurrent.TimeUnit;

@Slf4j
@AutoService(BaseFlinkSink.class)
public class FileSink implements FlinkStreamSink, FlinkBatchSink {

    private static final long serialVersionUID = -1648045076508797396L;

    private static final String PATH = "path";
    private static final String FORMAT = "format";
    private static final String WRITE_MODE = "write_mode";
    private static final String PARALLELISM = "parallelism";
    private static final String PATH_TIME_FORMAT = "path_time_format";
    private static final String DEFAULT_TIME_FORMAT = "yyyyMMddHHmmss";
    // *********************** For stream mode config ************************
    private static final String ROLLOVER_INTERVAL = "rollover_interval";
    private static final long DEFAULT_ROLLOVER_INTERVAL = 60;
    private static final String MAX_PART_SIZE = "max_part_size";
    private static final long DEFAULT_MAX_PART_SIZE = 1024;
    private static final String PART_PREFIX = "prefix";
    private static final String DEFAULT_PART_PREFIX = "seatunnel";
    private static final String PART_SUFFIX = "suffix";
    private static final String DEFAULT_PART_SUFFIX = ".ext";
    private static final long MB = 1024 * 1024;
    // ***********************************************************************

    private Config config;

    private FileOutputFormat<Row> outputFormat;

    private Path filePath;

    @Override
    public void outputStream(FlinkEnvironment env, DataStream<Row> dataStream) {
        final DefaultRollingPolicy<Row, String> rollingPolicy = DefaultRollingPolicy.builder()
                .withMaxPartSize(MB * TypesafeConfigUtils.getConfig(config, MAX_PART_SIZE, DEFAULT_MAX_PART_SIZE))
                .withRolloverInterval(
                        TimeUnit.MINUTES.toMillis(TypesafeConfigUtils.getConfig(config, ROLLOVER_INTERVAL, DEFAULT_ROLLOVER_INTERVAL)))
                .build();
        OutputFileConfig outputFileConfig = OutputFileConfig.builder()
                .withPartPrefix(TypesafeConfigUtils.getConfig(config, PART_PREFIX, DEFAULT_PART_PREFIX))
                .withPartSuffix(TypesafeConfigUtils.getConfig(config, PART_SUFFIX, DEFAULT_PART_SUFFIX))
                .build();

        final StreamingFileSink<Row> sink = StreamingFileSink
                .forRowFormat(filePath, new SimpleStringEncoder<Row>())
                .withRollingPolicy(rollingPolicy)
                .withOutputFileConfig(outputFileConfig)
                .build();
        dataStream.addSink(sink);
    }

    @Override
    public void outputBatch(FlinkEnvironment env, DataSet<Row> dataSet) {
        FormatType format = FormatType.from(config.getString(FORMAT).trim().toLowerCase());
        switch (format) {
            case JSON:
                RowTypeInfo rowTypeInfo = (RowTypeInfo) dataSet.getType();
                outputFormat = new JsonRowOutputFormat(filePath, rowTypeInfo);
                break;
            case CSV:
                outputFormat = new CsvRowOutputFormat(filePath);
                break;
            case TEXT:
                outputFormat = new TextOutputFormat<>(filePath);
                break;
            default:
                log.warn(" unknown file_format [{}],only support json,csv,text", format);
                break;

        }
        if (config.hasPath(WRITE_MODE)) {
            String mode = config.getString(WRITE_MODE);
            outputFormat.setWriteMode(FileSystem.WriteMode.valueOf(mode));
        }

        DataSink<Row> dataSink = dataSet.output(outputFormat);
        if (config.hasPath(PARALLELISM)) {
            int parallelism = config.getInt(PARALLELISM);
            dataSink.setParallelism(parallelism);
        }
    }

    @Override
    public void setConfig(Config config) {
        this.config = config;
    }

    @Override
    public Config getConfig() {
        return config;
    }

    @Override
    public CheckResult checkConfig() {
        return CheckConfigUtil.checkAllExists(config, PATH, FORMAT);
    }

    @Override
    public void prepare(FlinkEnvironment env) {
        String format = TypesafeConfigUtils.getConfig(config, PATH_TIME_FORMAT, DEFAULT_TIME_FORMAT);
        String path = VariablesSubstitute.substitute(config.getString(PATH), format);
        filePath = new Path(path);
    }

    @Override
    public void close() throws Exception {
        if (outputFormat != null) {
            outputFormat.close();
        }
    }

    @Override
    public String getPluginName() {
        return "FileSink";
    }
}
