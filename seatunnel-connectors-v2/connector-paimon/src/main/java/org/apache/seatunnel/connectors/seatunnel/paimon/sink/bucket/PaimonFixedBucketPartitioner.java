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

package org.apache.seatunnel.connectors.seatunnel.paimon.sink.bucket;

import org.apache.seatunnel.api.sink.SinkDataPartitioner;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.paimon.utils.RowConverter;

import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.sink.FixedBucketWriteSelector;

import java.util.Optional;

/** Routes records for a fixed-bucket Paimon table to their unique writer owner. */
public class PaimonFixedBucketPartitioner implements SinkDataPartitioner<SeaTunnelRow> {

    private static final long serialVersionUID = 1L;

    private final SeaTunnelRowType sourceRowType;
    private final TableSchema sinkTableSchema;
    private final int writerCount;
    private final FixedBucketWriteSelector writeSelector;
    private final String targetIdentifier;

    public PaimonFixedBucketPartitioner(
            SeaTunnelRowType sourceRowType, TableSchema sinkTableSchema, int writerCount) {
        this(sourceRowType, sinkTableSchema, writerCount, null);
    }

    /** Includes the physical table identity so wrappers can reject independent bucket owners. */
    public PaimonFixedBucketPartitioner(
            SeaTunnelRowType sourceRowType,
            TableSchema sinkTableSchema,
            int writerCount,
            String targetIdentifier) {
        this.sourceRowType = sourceRowType;
        this.sinkTableSchema = sinkTableSchema;
        this.writerCount = writerCount;
        this.writeSelector = new FixedBucketWriteSelector(sinkTableSchema);
        this.targetIdentifier = targetIdentifier;
    }

    @Override
    public int select(SeaTunnelRow record) {
        return writeSelector.select(
                RowConverter.reconvert(record, sourceRowType, sinkTableSchema), writerCount);
    }

    @Override
    public Optional<String> targetIdentifier() {
        return Optional.ofNullable(targetIdentifier);
    }
}
