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

package org.apache.seatunnel.connectors.seatunnel.deltalake.source.reader;

import io.delta.kernel.Scan;
import io.delta.kernel.ScanBuilder;
import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import lombok.Builder;
import lombok.NonNull;
import org.apache.seatunnel.connectors.seatunnel.deltalake.exception.DeltalakeConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.deltalake.exception.DeltalakeConnectorException;
import org.apache.seatunnel.connectors.seatunnel.deltalake.source.split.DeltaLakeFileScanTaskSplit;

import java.io.Closeable;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.lang.String.format;


@Builder
public class DeltaLakeFileScanTaskReader implements Closeable {


    private final Engine engine;
    private final List<String> columnsOpt;

    public CloseableIterator<Row> open(@NonNull DeltaLakeFileScanTaskSplit split) {
        try {
            Table table = Table.forPath(engine, split.getTablePath().toString());
            Snapshot snapshot = table.getLatestSnapshot(engine);
            StructType readSchema = pruneSchema(
                    snapshot.getSchema(engine), Optional.ofNullable(columnsOpt)
            );

            ScanBuilder scanBuilder = snapshot
                    .getScanBuilder(engine)
                    .withReadSchema(engine, readSchema);

            if (split.getResidualPredicate() != null) {
                scanBuilder = scanBuilder.withFilter(engine, split.getResidualPredicate());
            }
            Scan scan = scanBuilder.build();

            CloseableIterator<FilteredColumnarBatch> batch = scan.getScanFiles(engine);

            return new BatchToRowIterator(batch);
        } catch (Exception e) {
            throw new DeltalakeConnectorException(
                    DeltalakeConnectorErrorCode.FILE_SCAN_SPLIT_FAILED,
                    "Failed to open Delta Lake file scan split: " + split, e);
        }
    }

    /**
     * Utility method to return a pruned schema that contains the given {@code columns} from
     * {@code baseSchema}
     */
    protected static StructType pruneSchema(StructType baseSchema, Optional<List<String>> columns) {
        if (columns.isEmpty()) {
            return baseSchema;
        }
        List<StructField> selectedFields = columns.get().stream().map(column -> {
            if (baseSchema.indexOf(column) == -1) {
                throw new IllegalArgumentException(
                        format("Column %s is not found in table", column));
            }
            return baseSchema.get(column);
        }).collect(Collectors.toList());

        return new StructType(selectedFields);
    }

    @Override
    public void close() {
    }
}
