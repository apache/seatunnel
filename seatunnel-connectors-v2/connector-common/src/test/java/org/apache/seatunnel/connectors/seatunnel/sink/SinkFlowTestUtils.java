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

package org.apache.seatunnel.connectors.seatunnel.sink;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.DefaultSinkWriterContext;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SinkCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.JobMode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class SinkFlowTestUtils {

    public static void runBatchWithCheckpointDisabled(
            CatalogTable catalogTable,
            ReadonlyConfig options,
            TableSinkFactory<SeaTunnelRow, ?, ?, ?> factory,
            List<SeaTunnelRow> rows)
            throws IOException {
        JobContext context = new JobContext(System.currentTimeMillis());
        context.setJobMode(JobMode.BATCH);
        context.setEnableCheckpoint(false);
        runWithContext(catalogTable, options, factory, rows, context, 1);
    }

    public static void runBatchWithCheckpointEnabled(
            CatalogTable catalogTable,
            ReadonlyConfig options,
            TableSinkFactory<SeaTunnelRow, ?, ?, ?> factory,
            List<SeaTunnelRow> rows)
            throws IOException {
        JobContext context = new JobContext(System.currentTimeMillis());
        context.setJobMode(JobMode.BATCH);
        context.setEnableCheckpoint(true);
        // TODO trigger checkpoint with interval
        runWithContext(catalogTable, options, factory, rows, context, 1);
    }

    public static void runParallelSubtasksBatchWithCheckpointDisabled(
            CatalogTable catalogTable,
            ReadonlyConfig options,
            TableSinkFactory<SeaTunnelRow, ?, ?, ?> factory,
            List<SeaTunnelRow> rows,
            int parallelism)
            throws IOException {
        JobContext context = new JobContext(System.currentTimeMillis());
        context.setJobMode(JobMode.BATCH);
        context.setEnableCheckpoint(false);
        runWithContext(catalogTable, options, factory, rows, context, parallelism);
    }

    private static void runWithContext(
            CatalogTable catalogTable,
            ReadonlyConfig options,
            TableSinkFactory<SeaTunnelRow, ?, ?, ?> factory,
            List<SeaTunnelRow> rows,
            JobContext context,
            int parallelism)
            throws IOException {
        SeaTunnelSink<SeaTunnelRow, ?, ?, ?> sink =
                factory.createSink(
                                new TableSinkFactoryContext(
                                        catalogTable,
                                        options,
                                        Thread.currentThread().getContextClassLoader()))
                        .createSink();
        sink.setJobContext(context);
        List<Object> commitInfos = new ArrayList<>();
        for (int i = 0; i < parallelism; i++) {
            SinkWriter<SeaTunnelRow, ?, ?> sinkWriter =
                    sink.createWriter(new DefaultSinkWriterContext(i, parallelism));
            for (SeaTunnelRow row : rows) {
                sinkWriter.write(row);
            }
            Optional<?> commitInfo = sinkWriter.prepareCommit(1);
            sinkWriter.snapshotState(1);
            sinkWriter.close();
            if (commitInfo.isPresent()) {
                commitInfos.add(commitInfo.get());
            }
        }

        Optional<? extends SinkCommitter<?>> sinkCommitter = sink.createCommitter();
        Optional<? extends SinkAggregatedCommitter<?, ?>> aggregatedCommitter =
                sink.createAggregatedCommitter();

        if (!commitInfos.isEmpty()) {
            if (aggregatedCommitter.isPresent()) {
                Object aggregatedCommitInfoT =
                        ((SinkAggregatedCommitter) aggregatedCommitter.get()).combine(commitInfos);
                ((SinkAggregatedCommitter) aggregatedCommitter.get())
                        .commit(Collections.singletonList(aggregatedCommitInfoT));
                aggregatedCommitter.get().close();
            } else if (sinkCommitter.isPresent()) {
                ((SinkCommitter) sinkCommitter.get()).commit(commitInfos);
            } else {
                throw new RuntimeException("No committer found");
            }
        }
    }
}
