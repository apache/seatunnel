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

package org.apache.seatunnel.connectors.seatunnel.cdc.tidb.source.config;

import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;

import org.tikv.common.TiConfiguration;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode
public class TiDBSourceConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    private String databaseName;
    private String tableName;
    private List<String> tableNames;
    private StartupMode startupMode;
    private TiConfiguration tiConfiguration;
    private Integer batchSize;

    /**
     * Returns the captured tables in {@code database_name.table_name} format, falling back to the
     * legacy single {@code databaseName}/{@code tableName} pair when {@code tableNames} is absent.
     *
     * @return list of table full names, never null
     */
    public List<String> getTableFullNames() {
        if (tableNames != null && !tableNames.isEmpty()) {
            return tableNames;
        }
        if (databaseName != null && tableName != null) {
            return Collections.singletonList(
                    TiDBSourceOptions.tableFullName(databaseName, tableName));
        }
        throw new IllegalStateException(
                "TiDB source config has no table configured: neither tableNames nor"
                        + " databaseName/tableName is set.");
    }

    /** @return true if at least one table is configured via either option style. */
    public boolean hasConfiguredTables() {
        return (tableNames != null && !tableNames.isEmpty())
                || (databaseName != null && tableName != null);
    }
}
