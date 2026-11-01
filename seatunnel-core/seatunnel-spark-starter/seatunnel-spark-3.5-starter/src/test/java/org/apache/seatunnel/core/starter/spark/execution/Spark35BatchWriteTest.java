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

package org.apache.seatunnel.core.starter.spark.execution;

import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.translation.spark.sink.SeaTunnelBatchWrite;
import org.apache.seatunnel.translation.spark.sink.SparkSinkInjector;
import org.apache.seatunnel.translation.spark.utils.TypeConverterUtils;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Exercises commit coordination and a finite file stream through the Spark 3.5 sink adapter.
 *
 * <p>This does not cover streaming through the public starter or checkpoint restore.
 */
public class Spark35BatchWriteTest {

    @TempDir Path tempDir;

    @Test
    public void useCommitCoordinator() throws IOException {
        SeaTunnelBatchWrite<Void, Void, Void> batchWrite =
                new SeaTunnelBatchWrite<>(new TestSink(), new CatalogTable[0], "test", 1);

        Assertions.assertTrue(batchWrite.useCommitCoordinator());
    }

    @Test
    public void boundedMicroBatchCommitsRowsThroughStreamingAdapter() throws Exception {
        Assumptions.assumeFalse(
                System.getProperty("os.name").startsWith("Windows"),
                "Spark's local file stream requires Hadoop native libraries on Windows");
        Path input = Files.createDirectory(tempDir.resolve("input"));
        Files.write(
                input.resolve("rows.json"),
                Arrays.asList("{\"id\":1,\"name\":\"first\"}", "{\"id\":2,\"name\":\"second\"}"),
                StandardCharsets.UTF_8);
        Path committed = tempDir.resolve("committed.txt");
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name"},
                        new SeaTunnelDataType[] {BasicType.INT_TYPE, BasicType.STRING_TYPE});
        StructType parcelType = (StructType) TypeConverterUtils.parcel(rowType);
        SparkSession spark =
                SparkSession.builder()
                        .master("local[2]")
                        .appName("spark35-bounded-microbatch")
                        .config("spark.driver.host", "127.0.0.1")
                        .config("spark.driver.bindAddress", "127.0.0.1")
                        .config("spark.ui.enabled", "false")
                        .config("spark.sql.warehouse.dir", tempDir.resolve("warehouse").toString())
                        .config("spark.sql.shuffle.partitions", "2")
                        .getOrCreate();
        StreamingQuery query = null;
        try {
            // Exercise Spark's streaming adapter, not the batch-only public starter pipeline.
            Dataset<Row> rows =
                    spark.readStream()
                            .schema(
                                    new StructType()
                                            .add("id", DataTypes.IntegerType)
                                            .add("name", DataTypes.StringType))
                            .json(input.toString())
                            .map(
                                    (MapFunction<Row, Row>)
                                            row ->
                                                    RowFactory.create(
                                                            RowKind.INSERT.toByteValue(),
                                                            "test.test.test",
                                                            row.getInt(0),
                                                            row.getString(1)),
                                    SparkRowEncoder.create(parcelType));
            query =
                    SparkSinkInjector.inject(
                                    rows.writeStream(),
                                    new StreamingTestSink(committed.toString()),
                                    new CatalogTable[] {
                                        CatalogTableUtil.getCatalogTable(
                                                "test", "test", "test", "test", rowType)
                                    },
                                    spark.sparkContext().applicationId(),
                                    2)
                            .option("checkpointLocation", tempDir.resolve("checkpoint").toString())
                            .trigger(Trigger.Once())
                            .start();
            Assertions.assertTrue(query.awaitTermination(60000), "Micro-batch did not terminate");
            Assertions.assertFalse(query.isActive());
            Assertions.assertEquals(2, query.lastProgress().numInputRows());
            List<String> actual = Files.readAllLines(committed, StandardCharsets.UTF_8);
            Collections.sort(actual);
            Assertions.assertEquals(Arrays.asList("1:first:INSERT", "2:second:INSERT"), actual);
        } finally {
            try {
                if (query != null && query.isActive()) {
                    query.stop();
                }
            } finally {
                spark.close();
            }
        }
    }

    private static class StreamingTestSink
            implements SeaTunnelSink<SeaTunnelRow, Void, List<String>, List<String>> {

        private final String output;

        private StreamingTestSink(String output) {
            this.output = output;
        }

        @Override
        public String getPluginName() {
            return "StreamingTestSink";
        }

        @Override
        public SinkWriter<SeaTunnelRow, List<String>, Void> createWriter(
                SinkWriter.Context context) {
            return new SinkWriter<SeaTunnelRow, List<String>, Void>() {
                private final List<String> rows = new ArrayList<>();

                @Override
                public void write(SeaTunnelRow row) {
                    rows.add(row.getField(0) + ":" + row.getField(1) + ":" + row.getRowKind());
                }

                @Override
                public Optional<List<String>> prepareCommit() {
                    return Optional.of(new ArrayList<>(rows));
                }

                @Override
                public void abortPrepare() {}

                @Override
                public void close() {}
            };
        }

        @Override
        public Optional<SinkAggregatedCommitter<List<String>, List<String>>>
                createAggregatedCommitter() {
            return Optional.of(
                    new SinkAggregatedCommitter<List<String>, List<String>>() {
                        @Override
                        public List<List<String>> commit(List<List<String>> commits)
                                throws IOException {
                            Files.write(
                                    Paths.get(output), combine(commits), StandardCharsets.UTF_8);
                            return Collections.emptyList();
                        }

                        @Override
                        public List<String> combine(List<List<String>> commits) {
                            List<String> rows = new ArrayList<>();
                            commits.forEach(rows::addAll);
                            return rows;
                        }

                        @Override
                        public void abort(List<List<String>> commits) {
                            throw new AssertionError("Unexpected micro-batch abort");
                        }

                        @Override
                        public void close() {}
                    });
        }
    }

    private static class TestSink implements SeaTunnelSink<SeaTunnelRow, Void, Void, Void> {

        @Override
        public String getPluginName() {
            return "TestSink";
        }

        @Override
        public SinkWriter<SeaTunnelRow, Void, Void> createWriter(SinkWriter.Context context) {
            throw new UnsupportedOperationException();
        }
    }
}
