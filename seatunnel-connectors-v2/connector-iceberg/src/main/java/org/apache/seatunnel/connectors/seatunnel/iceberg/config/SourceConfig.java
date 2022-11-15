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

package org.apache.seatunnel.connectors.seatunnel.iceberg.config;

import static org.apache.seatunnel.connectors.seatunnel.iceberg.source.enumerator.scan.IcebergStreamScanStrategy.FROM_LATEST_SNAPSHOT;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.connectors.seatunnel.iceberg.source.enumerator.scan.IcebergStreamScanStrategy;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import lombok.Getter;
import lombok.ToString;
import org.apache.iceberg.expressions.Expression;

@Getter
@ToString
public class SourceConfig extends CommonConfig {
    private static final long serialVersionUID = -1965861967575264253L;

    public static final Option<Long> KEY_START_SNAPSHOT_TIMESTAMP = Options.key("start_snapshot_timestamp")
        .longType()
        .noDefaultValue()
        .withDescription(" the iceberg timestamp of starting snapshot ");

    public static final Option<Long> KEY_START_SNAPSHOT_ID = Options.key("start_snapshot_id")
        .longType()
        .noDefaultValue()
        .withDescription(" the iceberg id of starting snapshot ");

    public static final Option<Long> KEY_END_SNAPSHOT_ID = Options.key("end_snapshot_id")
        .longType()
        .noDefaultValue()
        .withDescription(" the iceberg id of ending snapshot ");

    public static final Option<Long> KEY_USE_SNAPSHOT_ID = Options.key("use_snapshot_id")
        .longType()
        .noDefaultValue()
        .withDescription(" the iceberg used snapshot id");

    public static final Option<Long> KEY_USE_SNAPSHOT_TIMESTAMP = Options.key("use_snapshot_timestamp")
        .longType()
        .noDefaultValue()
        .withDescription(" the iceberg used snapshot timestamp");

    public static final Option<IcebergStreamScanStrategy> KEY_STREAM_SCAN_STRATEGY = Options.key("stream_scan_strategy")
        .enumType(IcebergStreamScanStrategy.class)
        .defaultValue(FROM_LATEST_SNAPSHOT)
        .withDescription(" the iceberg strategy of stream scanning");

    private Long startSnapshotTimestamp;
    private Long startSnapshotId;
    private Long endSnapshotId;

    private Long useSnapshotId;
    private Long useSnapshotTimestamp;

    private IcebergStreamScanStrategy streamScanStrategy = KEY_STREAM_SCAN_STRATEGY.defaultValue();
    private Expression filter;
    private Long splitSize;
    private Integer splitLookback;
    private Long splitOpenFileCost;

    public SourceConfig(Config pluginConfig) {
        super(pluginConfig);
        if (pluginConfig.hasPath(KEY_START_SNAPSHOT_TIMESTAMP.key())) {
            this.startSnapshotTimestamp = pluginConfig.getLong(KEY_START_SNAPSHOT_TIMESTAMP.key());
        }
        if (pluginConfig.hasPath(KEY_START_SNAPSHOT_ID.key())) {
            this.startSnapshotId = pluginConfig.getLong(KEY_START_SNAPSHOT_ID.key());
        }
        if (pluginConfig.hasPath(KEY_END_SNAPSHOT_ID.key())) {
            this.endSnapshotId = pluginConfig.getLong(KEY_END_SNAPSHOT_ID.key());
        }
        if (pluginConfig.hasPath(KEY_USE_SNAPSHOT_ID.key())) {
            this.useSnapshotId = pluginConfig.getLong(KEY_USE_SNAPSHOT_ID.key());
        }
        if (pluginConfig.hasPath(KEY_USE_SNAPSHOT_TIMESTAMP.key())) {
            this.useSnapshotTimestamp = pluginConfig.getLong(KEY_USE_SNAPSHOT_TIMESTAMP.key());
        }
        if (pluginConfig.hasPath(KEY_STREAM_SCAN_STRATEGY.key())) {
            this.streamScanStrategy = pluginConfig.getEnum(
                IcebergStreamScanStrategy.class, KEY_STREAM_SCAN_STRATEGY.key());
        }
    }

    public static SourceConfig loadConfig(Config pluginConfig) {
        return new SourceConfig(pluginConfig);
    }
}
