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

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.seatunnel.iceberg.source.enumerator.scan.IcebergStreamScanStrategy;
import org.apache.seatunnel.connectors.seatunnel.iceberg.utils.SchemaUtils;

import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expression;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.experimental.Tolerate;

import java.io.Serializable;

import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.SourceConfig.KEY_STREAM_SCAN_STRATEGY;

@AllArgsConstructor
@Data
@Builder
public class SourceTableConfig implements Serializable {
    private String namespace;
    private String table;

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

    @Tolerate
    public SourceTableConfig() {}

    public TablePath getTablePath() {
        String[] paths = table.split("\\.");
        if (paths.length == 1) {
            return TablePath.of(namespace, table);
        }
        if (paths.length == 2) {
            return TablePath.of(paths[0], paths[1]);
        }
        String namespace = table.substring(0, table.lastIndexOf("\\."));
        return TablePath.of(namespace, table);
    }

    public TableIdentifier getTableIdentifier() {
        return SchemaUtils.toIcebergTableIdentifier(getTablePath());
    }

    public SourceTableConfig setNamespace(String namespace) {
        this.namespace = namespace;
        return this;
    }
}
