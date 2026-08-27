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

package org.apache.seatunnel.connectors.seatunnel.iceberg.source.reader;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.iceberg.data.Deserializer;
import org.apache.seatunnel.connectors.seatunnel.iceberg.exception.IcebergConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.iceberg.exception.IcebergConnectorException;
import org.apache.seatunnel.connectors.seatunnel.iceberg.source.split.IcebergFileScanTaskSplit;

import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterator;

import lombok.AllArgsConstructor;
import lombok.NonNull;

import java.io.Closeable;
import java.io.IOException;

@AllArgsConstructor
public class IcebergFileScanTaskSplitReader implements Closeable {

    private Deserializer deserializer;
    private IcebergFileScanTaskReader icebergFileScanTaskReader;

    public CloseableIterator<SeaTunnelRow> open(@NonNull IcebergFileScanTaskSplit split) {
        CloseableIterator<Record> iterator = icebergFileScanTaskReader.open(split.getTask());

        OffsetSeekIterator<Record> seekIterator = new OffsetSeekIterator<>(iterator);
        seekIterator.seek(split.getRecordOffset());

        String tableId = split.getTablePath().getFullName();
        return CloseableIterator.transform(
                seekIterator,
                record -> {
                    SeaTunnelRow seaTunnelRow = deserializer.deserialize(record);
                    seaTunnelRow.setTableId(tableId);
                    split.setRecordOffset(split.getRecordOffset() + 1);
                    return seaTunnelRow;
                });
    }

    @Override
    public void close() {
        icebergFileScanTaskReader.close();
    }

    @AllArgsConstructor
    private static class OffsetSeekIterator<T> implements CloseableIterator<T> {
        private final CloseableIterator<T> iterator;

        public void seek(long startingRecordOffset) {
            for (long i = 0; i < startingRecordOffset; ++i) {
                if (hasNext()) {
                    next();
                } else {
                    throw new IcebergConnectorException(
                            IcebergConnectorErrorCode.INVALID_STARTING_RECORD_OFFSET,
                            String.format(
                                    "Invalid starting record offset %d", startingRecordOffset));
                }
            }
        }

        @Override
        public void close() throws IOException {
            iterator.close();
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public T next() {
            return iterator.next();
        }
    }
}
