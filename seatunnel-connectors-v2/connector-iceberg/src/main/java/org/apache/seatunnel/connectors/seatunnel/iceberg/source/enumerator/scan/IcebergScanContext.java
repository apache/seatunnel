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

package org.apache.seatunnel.connectors.seatunnel.iceberg.source.enumerator.scan;

import org.apache.seatunnel.connectors.seatunnel.iceberg.config.SourceConfig;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;

@Getter
@Builder(toBuilder = true)
@ToString
public class IcebergScanContext {

    private final boolean streaming;
    private final IcebergStreamScanStrategy streamScanStrategy;

    private final Long startSnapshotId;
    private final Long startSnapshotTimestamp;
    private final Long endSnapshotId;

    private final Long useSnapshotId;
    private final Long useSnapshotTimestamp;

    private final boolean caseSensitive;

    private final Schema schema;
    private final Expression filter;
    private final Long splitSize;
    private final Integer splitLookback;
    private final Long splitOpenFileCost;

    public IcebergScanContext copyWithAppendsBetween(Long newStartSnapshotId,
                                                     long newEndSnapshotId) {
        return this.toBuilder()
            .useSnapshotId(null)
            .useSnapshotTimestamp(null)
            .startSnapshotId(newStartSnapshotId)
            .endSnapshotId(newEndSnapshotId)
            .build();
    }

    public static IcebergScanContext scanContext(SourceConfig sourceConfig,
                                                 Schema schema) {
        return IcebergScanContext.builder()
            .startSnapshotTimestamp(sourceConfig.getStartSnapshotTimestamp())
            .startSnapshotId(sourceConfig.getStartSnapshotId())
            .endSnapshotId(sourceConfig.getEndSnapshotId())
            .useSnapshotId(sourceConfig.getUseSnapshotId())
            .useSnapshotTimestamp(sourceConfig.getUseSnapshotTimestamp())
            .caseSensitive(sourceConfig.isCaseSensitive())
            .schema(schema)
            .filter(sourceConfig.getFilter())
            .splitSize(sourceConfig.getSplitSize())
            .splitLookback(sourceConfig.getSplitLookback())
            .splitOpenFileCost(sourceConfig.getSplitOpenFileCost())
            .build();
    }

    public static IcebergScanContext streamScanContext(SourceConfig sourceConfig,
                                                       Schema schema) {
        return scanContext(sourceConfig, schema).toBuilder()
            .streaming(true)
            .streamScanStrategy(sourceConfig.getStreamScanStrategy())
            .build();
    }
}
