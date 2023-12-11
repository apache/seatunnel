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

package org.apache.seatunnel.translation.flink.source;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.translation.flink.serialization.FlinkRowConverter;

import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.types.Row;

/**
 * The implementation of {@link Collector} for flink engine, as a container for {@link SeaTunnelRow}
 * and convert {@link SeaTunnelRow} to {@link Row}.
 */
public class FlinkRowCollector implements Collector<SeaTunnelRow> {

    private ReaderOutput<Row> readerOutput;

    private final FlinkRowConverter rowSerialization;

    public FlinkRowCollector(SeaTunnelRowType seaTunnelRowType) {
        this.rowSerialization = new FlinkRowConverter(seaTunnelRowType);
    }

    @Override
    public void collect(SeaTunnelRow record) {
        try {
            readerOutput.collect(rowSerialization.convert(record));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object getCheckpointLock() {
        return this;
    }

    public FlinkRowCollector withReaderOutput(ReaderOutput<Row> readerOutput) {
        this.readerOutput = readerOutput;
        return this;
    }
}
