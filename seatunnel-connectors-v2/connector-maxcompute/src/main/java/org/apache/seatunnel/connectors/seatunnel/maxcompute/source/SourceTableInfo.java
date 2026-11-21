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

package org.apache.seatunnel.connectors.seatunnel.maxcompute.source;

import org.apache.seatunnel.api.table.catalog.CatalogTable;

import lombok.Getter;

import java.io.Serializable;

@Getter
public class SourceTableInfo implements Serializable {
    private final CatalogTable catalogTable;
    private final String partitionSpec;
    private final Integer splitRow;

    /**
     * Whether to ignore missing partitions during split enumeration.
     *
     * <p>Behavioral contract:
     *
     * <ul>
     *   <li>When consumed: Consumed by {@code MaxcomputeSourceSplitEnumerator#discoverySplits()}
     *       during split discovery.
     *   <li>Partition-only suppression: When {@code true}, if the configured {@code partitionSpec}
     *       does not exist on an existing table, download session creation is suppressed and record
     *       count is treated as 0. If the table itself does not exist, an error is thrown
     *       (suppression never applies to missing tables).
     *   <li>Inheritance: Defaults to {@code false}. Inherits from global {@code
     *       ignore_missing_partition} option or can be overridden per table in {@code table_list}.
     * </ul>
     */
    private final boolean ignoreMissingPartition;

    public SourceTableInfo(CatalogTable catalogTable, String partitionSpec, Integer splitRow) {
        this(catalogTable, partitionSpec, splitRow, false);
    }

    public SourceTableInfo(
            CatalogTable catalogTable,
            String partitionSpec,
            Integer splitRow,
            boolean ignoreMissingPartition) {
        this.catalogTable = catalogTable;
        this.partitionSpec = partitionSpec;
        this.splitRow = splitRow;
        this.ignoreMissingPartition = ignoreMissingPartition;
    }
}
