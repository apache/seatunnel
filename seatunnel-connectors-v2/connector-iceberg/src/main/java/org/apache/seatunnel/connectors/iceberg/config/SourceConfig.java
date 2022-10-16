/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.seatunnel.connectors.iceberg.config;

import org.apache.seatunnel.connectors.iceberg.source.enumerator.scan.IcebergStreamScanStrategy;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import lombok.Getter;
import lombok.ToString;
import org.apache.iceberg.expressions.Expression;

@Getter
@ToString
public class SourceConfig extends CommonConfig {
    private static final long serialVersionUID = -1965861967575264253L;

    private static final String KEY_START_SNAPSHOT_TIMESTAMP = "start_snapshot_timestamp";
    private static final String KEY_START_SNAPSHOT_ID = "start_snapshot_id";
    private static final String KEY_END_SNAPSHOT_ID = "end_snapshot_id";
    private static final String KEY_USE_SNAPSHOT_ID = "use_snapshot_id";
    private static final String KEY_USE_SNAPSHOT_TIMESTAMP = "use_snapshot_timestamp";
    private static final String KEY_STREAM_SCAN_STRATEGY = "stream_scan_strategy";

    private Long startSnapshotTimestamp;
    private Long startSnapshotId;
    private Long endSnapshotId;

    private Long useSnapshotId;
    private Long useSnapshotTimestamp;

    private IcebergStreamScanStrategy streamScanStrategy = IcebergStreamScanStrategy.FROM_LATEST_SNAPSHOT;
    private Expression filter;
    private Long splitSize;
    private Integer splitLookback;
    private Long splitOpenFileCost;

    public SourceConfig(Config pluginConfig) {
        super(pluginConfig);
        if (pluginConfig.hasPath(KEY_START_SNAPSHOT_TIMESTAMP)) {
            this.startSnapshotTimestamp = pluginConfig.getLong(KEY_START_SNAPSHOT_TIMESTAMP);
        }
        if (pluginConfig.hasPath(KEY_START_SNAPSHOT_ID)) {
            this.startSnapshotId = pluginConfig.getLong(KEY_START_SNAPSHOT_ID);
        }
        if (pluginConfig.hasPath(KEY_END_SNAPSHOT_ID)) {
            this.endSnapshotId = pluginConfig.getLong(KEY_END_SNAPSHOT_ID);
        }
        if (pluginConfig.hasPath(KEY_USE_SNAPSHOT_ID)) {
            this.useSnapshotId = pluginConfig.getLong(KEY_USE_SNAPSHOT_ID);
        }
        if (pluginConfig.hasPath(KEY_USE_SNAPSHOT_TIMESTAMP)) {
            this.useSnapshotTimestamp = pluginConfig.getLong(KEY_USE_SNAPSHOT_TIMESTAMP);
        }
        if (pluginConfig.hasPath(KEY_STREAM_SCAN_STRATEGY)) {
            this.streamScanStrategy = pluginConfig.getEnum(
                IcebergStreamScanStrategy.class, KEY_STREAM_SCAN_STRATEGY);
        }
    }

    public static SourceConfig loadConfig(Config pluginConfig) {
        return new SourceConfig(pluginConfig);
    }
}
