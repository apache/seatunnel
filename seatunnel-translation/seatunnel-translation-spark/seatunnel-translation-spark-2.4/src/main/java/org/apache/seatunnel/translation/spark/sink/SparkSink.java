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

import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.Constants;
import org.apache.seatunnel.common.utils.SerializationUtils;
import org.apache.seatunnel.translation.spark.sink.writer.SparkDataSourceWriter;
import org.apache.seatunnel.translation.spark.sink.writer.SparkStreamWriter;

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
import java.util.Optional;

public class SparkSink<StateT, CommitInfoT, AggregatedCommitInfoT>
        implements WriteSupport, StreamWriteSupport, DataSourceV2 {

    private volatile SeaTunnelSink<SeaTunnelRow, StateT, CommitInfoT, AggregatedCommitInfoT> sink;

    private volatile CatalogTable[] catalogTables;

    private volatile String jobId;

    private volatile Integer parallelism;

    private void init(DataSourceOptions options) {
        if (sink == null) {
            this.sink =
                    SerializationUtils.stringToObject(
                            options.get(Constants.SINK_SERIALIZATION)
                                    .orElseThrow(
                                            () ->
                                                    new IllegalArgumentException(
                                                            "can not find sink "
                                                                    + "class string in DataSourceOptions")));
        }
        if (catalogTables == null) {
            this.catalogTables =
                    SerializationUtils.stringToObject(
                            options.get(SparkSinkInjector.SINK_CATALOG_TABLE)
                                    .orElseThrow(
                                            () ->
                                                    new IllegalArgumentException(
                                                            "can not find sink "
                                                                    + "catalog table string in DataSourceOptions")));
        }
        if (jobId == null) {
            this.jobId = options.get(SparkSinkInjector.JOB_ID).orElse(null);
        }
        if (parallelism == null) {
            this.parallelism =
                    options.get(SparkSinkInjector.PARALLELISM)
                            .map(Integer::parseInt)
                            .orElseThrow(
                                    () ->
                                            new IllegalArgumentException(
                                                    SparkSinkInjector.PARALLELISM
                                                            + " must be specified"));
        }
    }

    @Override
    public StreamWriter createStreamWriter(
            String queryId, StructType schema, OutputMode mode, DataSourceOptions options) {
        init(options);

        try {
            return new SparkStreamWriter<>(sink, catalogTables, jobId, parallelism);
        } catch (IOException e) {
            throw new RuntimeException("find error when createStreamWriter", e);
        }
    }

    @Override
    public Optional<DataSourceWriter> createWriter(
            String writeUUID, StructType schema, SaveMode mode, DataSourceOptions options) {
        init(options);

        try {
            return Optional.of(
                    new SparkDataSourceWriter<>(sink, catalogTables, jobId, parallelism));
        } catch (IOException e) {
            throw new RuntimeException("find error when createStreamWriter", e);
        }
    }
}
