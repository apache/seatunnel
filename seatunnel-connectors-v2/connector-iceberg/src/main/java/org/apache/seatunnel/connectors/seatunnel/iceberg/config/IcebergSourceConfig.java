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

package org.apache.seatunnel.connectors.seatunnel.iceberg.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.TablePath;

import lombok.Getter;
import lombok.ToString;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Getter
@ToString
public class IcebergSourceConfig extends IcebergCommonConfig {

    private static final long serialVersionUID = -1965861967575264253L;

    private long incrementScanInterval;
    private List<SourceTableConfig> tableList;

    public IcebergSourceConfig(ReadonlyConfig readonlyConfig) {
        super(readonlyConfig);
        this.incrementScanInterval =
                readonlyConfig.get(IcebergSourceOptions.KEY_INCREMENT_SCAN_INTERVAL);
        if (this.getTable() != null) {
            SourceTableConfig tableConfig =
                    SourceTableConfig.builder()
                            .namespace(this.getNamespace())
                            .table(this.getTable())
                            .startSnapshotTimestamp(
                                    readonlyConfig.get(
                                            IcebergSourceOptions.KEY_START_SNAPSHOT_TIMESTAMP))
                            .startSnapshotId(
                                    readonlyConfig.get(IcebergSourceOptions.KEY_START_SNAPSHOT_ID))
                            .endSnapshotId(
                                    readonlyConfig.get(IcebergSourceOptions.KEY_END_SNAPSHOT_ID))
                            .useSnapshotId(
                                    readonlyConfig.get(IcebergSourceOptions.KEY_USE_SNAPSHOT_ID))
                            .useSnapshotTimestamp(
                                    readonlyConfig.get(
                                            IcebergSourceOptions.KEY_USE_SNAPSHOT_TIMESTAMP))
                            .streamScanStrategy(
                                    readonlyConfig.get(
                                            IcebergSourceOptions.KEY_STREAM_SCAN_STRATEGY))
                            .build();
            this.tableList = Collections.singletonList(tableConfig);
        } else {
            this.tableList =
                    readonlyConfig.get(IcebergSourceOptions.KEY_TABLE_LIST).stream()
                            .map(tableConfig -> tableConfig.setNamespace(this.getNamespace()))
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
