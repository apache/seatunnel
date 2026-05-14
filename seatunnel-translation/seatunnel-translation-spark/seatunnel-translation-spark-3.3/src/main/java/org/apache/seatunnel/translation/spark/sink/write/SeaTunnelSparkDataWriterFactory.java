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

package org.apache.seatunnel.translation.spark.sink.write;

import org.apache.seatunnel.api.event.DefaultEventProcessor;
import org.apache.seatunnel.api.sink.DefaultSinkWriterContext;
import org.apache.seatunnel.api.sink.DirtyDataAwareSinkWriter;
import org.apache.seatunnel.api.sink.DirtyRecordCollector;
import org.apache.seatunnel.api.sink.DistributedCounter;
import org.apache.seatunnel.api.sink.NoOpDirtyRecordCollector;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.translation.spark.execution.MultiTableManager;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory;
import org.apache.spark.util.LongAccumulator;

import lombok.extern.slf4j.Slf4j;
import scala.Option;

import java.io.IOException;
import java.sql.DriverManager;

@Slf4j
public class SeaTunnelSparkDataWriterFactory<CommitInfoT, StateT>
        implements DataWriterFactory, StreamingDataWriterFactory {

    static {
        // Load DriverManager first to avoid deadlock between DriverManager's
        // static initialization block and specific driver class's static
        // initialization block when two different driver classes are loading
        // concurrently using Class.forName while DriverManager is uninitialized
        // before.
        //
        // This could happen in JDK 8 but not above as driver loading has been
        // moved out of DriverManager's static initialization block since JDK 9.
        DriverManager.getDrivers();
    }

    private final SeaTunnelSink<SeaTunnelRow, StateT, CommitInfoT, ?> sink;
    private final CatalogTable[] catalogTables;
    private final String jobId;
    private final int parallelism;
    private final DirtyRecordCollector dirtyRecordCollector;
    private final LongAccumulator dirtyRecordAccumulator;

    public SeaTunnelSparkDataWriterFactory(
            SeaTunnelSink<SeaTunnelRow, StateT, CommitInfoT, ?> sink,
            CatalogTable[] catalogTables,
            String jobId,
            int parallelism,
            DirtyRecordCollector dirtyRecordCollector) {
        this.sink = sink;
        this.catalogTables = catalogTables;
        this.jobId = jobId;
        this.parallelism = parallelism;
        this.dirtyRecordCollector =
                dirtyRecordCollector != null
                        ? dirtyRecordCollector
                        : NoOpDirtyRecordCollector.INSTANCE;

        this.dirtyRecordAccumulator = createSparkAccumulator(jobId);
        if (this.dirtyRecordAccumulator != null) {
            log.info("Created Spark LongAccumulator for distributed dirty record counting");
        }
    }

    private LongAccumulator createSparkAccumulator(String jobId) {
        try {
            Option<LongAccumulator> sparkAccumulator =
                    SparkSession.getActiveSession()
                            .map(
                                    session ->
                                            session.sparkContext()
                                                    .longAccumulator("dirtyRecordCount_" + jobId));
            return sparkAccumulator.get();
        } catch (Exception e) {
            log.warn("Failed to create Spark accumulator for dirty record counting", e);
            return null;
        }
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
        if (dirtyRecordAccumulator != null
                && !(dirtyRecordCollector instanceof NoOpDirtyRecordCollector)) {
            dirtyRecordCollector.setDistributedCounter(
                    new DistributedCounter() {
                        private static final long serialVersionUID = 1L;

                        @Override
                        public void add(long delta) {
                            dirtyRecordAccumulator.add(delta);
                        }

                        @Override
                        public long value() {
                            return dirtyRecordAccumulator.value();
                        }
                    });
            log.debug(
                    "Set Spark accumulator to dirty record collector for partition {}",
                    partitionId);
        }

        SinkWriter.Context context =
                new DefaultSinkWriterContext(
                        (int) taskId,
                        parallelism,
                        new DefaultEventProcessor(jobId),
                        dirtyRecordCollector);
        SinkWriter<SeaTunnelRow, CommitInfoT, StateT> writer;
        SinkCommitter<CommitInfoT> committer;
        try {
            writer = sink.createWriter(context);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create SinkWriter.", e);
        }
        if (!(dirtyRecordCollector instanceof NoOpDirtyRecordCollector)) {
            writer =
                    new DirtyDataAwareSinkWriter<>(
                            writer,
                            dirtyRecordCollector,
                            context.getIndexOfSubtask(),
                            catalogTables != null && catalogTables.length == 1
                                    ? catalogTables[0]
                                    : null);
        }
        try {
            committer = sink.createCommitter().orElse(null);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create SinkCommitter.", e);
        }
        return new SeaTunnelSparkDataWriter<>(
                writer, committer, new MultiTableManager(catalogTables), 0, context);
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId, long epochId) {
        return createWriter(partitionId, taskId);
    }
}
