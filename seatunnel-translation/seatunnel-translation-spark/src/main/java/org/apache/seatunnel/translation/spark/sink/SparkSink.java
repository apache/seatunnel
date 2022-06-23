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

package org.apache.seatunnel.translation.spark.sink;

import org.apache.seatunnel.api.sink.DefaultSinkWriterContext;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.common.utils.SerializationUtils;

import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.StreamWriteSupport;
import org.apache.spark.sql.sources.v2.WriteSupport;
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter;
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

public class SparkSink<InputT, StateT, CommitInfoT, AggregatedCommitInfoT> implements WriteSupport,
        StreamWriteSupport, DataSourceV2 {

    private volatile SeaTunnelSink<InputT, StateT, CommitInfoT, AggregatedCommitInfoT> sink;

    private String sinkString;
    private Map<String, String> configuration;

    private void init(DataSourceOptions options) {
        if (sink == null) {
            sinkString = options.get("sink").orElseThrow(() -> new IllegalArgumentException("can not find " +
                    "sink class string in DataSourceOptions"));
            this.sink = SerializationUtils.stringToObject(sinkString);
            this.configuration = SerializationUtils.stringToObject(
                    options.get("configuration").orElseThrow(() -> new IllegalArgumentException("can not " +
                            "find configuration class string in DataSourceOptions")));
        }
    }

    @Override
    public StreamWriter createStreamWriter(String queryId, StructType schema, OutputMode mode, DataSourceOptions options) {

        init(options);
        // TODO add subtask and parallelism.
        org.apache.seatunnel.api.sink.SinkWriter.Context stContext =
                new DefaultSinkWriterContext(configuration, 0, 0);

        try {
            return new SparkStreamWriterConverter(stContext, sink.createCommitter().orElse(null),
                    sink.createAggregatedCommitter().orElse(null), schema, sinkString).convert(sink.createWriter(stContext));
        } catch (IOException e) {
            throw new RuntimeException("find error when createStreamWriter", e);
        }
    }

    @Override
    public Optional<DataSourceWriter> createWriter(String writeUUID, StructType schema, SaveMode mode, DataSourceOptions options) {

        init(options);
        // TODO add subtask and parallelism.
        org.apache.seatunnel.api.sink.SinkWriter.Context stContext =
                new DefaultSinkWriterContext(configuration, 0, 0);

        try {
            return Optional.of(new SparkDataSourceWriterConverter(stContext, sink.createCommitter().orElse(null),
                    sink.createAggregatedCommitter().orElse(null), schema, sinkString).convert(sink.createWriter(stContext)));
        } catch (IOException e) {
            throw new RuntimeException("find error when createStreamWriter", e);
        }
    }
}
