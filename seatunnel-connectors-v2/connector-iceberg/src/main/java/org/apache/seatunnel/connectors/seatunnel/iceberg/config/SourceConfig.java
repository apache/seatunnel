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

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.seatunnel.iceberg.source.enumerator.scan.IcebergStreamScanStrategy;

import lombok.Getter;
import lombok.ToString;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.seatunnel.connectors.seatunnel.iceberg.source.enumerator.scan.IcebergStreamScanStrategy.FROM_LATEST_SNAPSHOT;

@Getter
@ToString
public class SourceConfig extends CommonConfig {
    private static final long serialVersionUID = -1965861967575264253L;

    public static final Option<Long> KEY_START_SNAPSHOT_TIMESTAMP =
            Options.key("start_snapshot_timestamp")
                    .longType()
                    .noDefaultValue()
                    .withDescription(" the iceberg timestamp of starting snapshot ");

    public static final Option<Long> KEY_START_SNAPSHOT_ID =
            Options.key("start_snapshot_id")
                    .longType()
                    .noDefaultValue()
                    .withDescription(" the iceberg id of starting snapshot ");

    public static final Option<Long> KEY_END_SNAPSHOT_ID =
            Options.key("end_snapshot_id")
                    .longType()
                    .noDefaultValue()
                    .withDescription(" the iceberg id of ending snapshot ");

    public static final Option<Long> KEY_USE_SNAPSHOT_ID =
            Options.key("use_snapshot_id")
                    .longType()
                    .noDefaultValue()
                    .withDescription(" the iceberg used snapshot id");

    public static final Option<Long> KEY_USE_SNAPSHOT_TIMESTAMP =
            Options.key("use_snapshot_timestamp")
                    .longType()
                    .noDefaultValue()
                    .withDescription(" the iceberg used snapshot timestamp");

    public static final Option<IcebergStreamScanStrategy> KEY_STREAM_SCAN_STRATEGY =
            Options.key("stream_scan_strategy")
                    .enumType(IcebergStreamScanStrategy.class)
                    .defaultValue(FROM_LATEST_SNAPSHOT)
                    .withDescription(" the iceberg strategy of stream scanning");

    public static final Option<List<SourceTableConfig>> KEY_TABLE_LIST =
            Options.key("table_list")
                    .listType(SourceTableConfig.class)
                    .noDefaultValue()
                    .withDescription(" the iceberg tables");

    public static final Option<Long> KEY_INCREMENT_SCAN_INTERVAL =
            Options.key("increment.scan-interval")
                    .longType()
                    .defaultValue(2000L)
                    .withDescription(" the interval of increment scan(mills)");

    private long incrementScanInterval;
    private List<SourceTableConfig> tableList;

    public SourceConfig(ReadonlyConfig readonlyConfig) {
        super(readonlyConfig);
        this.incrementScanInterval = readonlyConfig.get(KEY_INCREMENT_SCAN_INTERVAL);
        if (this.getTable() != null) {
            SourceTableConfig tableConfig =
                    SourceTableConfig.builder()
                            .namespace(this.getNamespace())
                            .table(this.getTable())
                            .startSnapshotTimestamp(
                                    readonlyConfig.get(KEY_START_SNAPSHOT_TIMESTAMP))
                            .startSnapshotId(readonlyConfig.get(KEY_START_SNAPSHOT_ID))
                            .endSnapshotId(readonlyConfig.get(KEY_END_SNAPSHOT_ID))
                            .useSnapshotId(readonlyConfig.get(KEY_USE_SNAPSHOT_ID))
                            .useSnapshotTimestamp(readonlyConfig.get(KEY_USE_SNAPSHOT_TIMESTAMP))
                            .streamScanStrategy(readonlyConfig.get(KEY_STREAM_SCAN_STRATEGY))
                            .build();
            this.tableList = Collections.singletonList(tableConfig);
        } else {
            this.tableList =
                    readonlyConfig.get(KEY_TABLE_LIST).stream()
                            .map(
                                    tableConfig ->
                                            tableConfig.setNamespace(
                                                    SourceConfig.this.getNamespace()))
                            .collect(Collectors.toList());
        }
    }

    public SourceTableConfig getTableConfig(TablePath tablePath) {
        return tableList.stream()
                .filter(tableConfig -> tableConfig.getTablePath().equals(tablePath))
                .findFirst()
                .get();
    }
}
