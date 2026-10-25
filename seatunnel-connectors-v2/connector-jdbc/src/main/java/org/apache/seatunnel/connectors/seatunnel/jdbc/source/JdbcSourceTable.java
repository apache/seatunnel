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

package org.apache.seatunnel.connectors.seatunnel.jdbc.source;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;

@Data
@Builder
public class JdbcSourceTable implements Serializable {
    private static final long serialVersionUID = 1L;

    private final TablePath tablePath;
    private final String query;
    private final String partitionColumn;
    private final Integer partitionNumber;
    private final String partitionStart;
    private final String partitionEnd;
    private final Boolean useSelectCount;
    private final Boolean skipAnalyze;
    private final CatalogTable catalogTable;

    /**
     * Returns a copy of this table with its query replaced by {@code query}, preserving every other
     * field. Used to scope split-metadata queries to a where-condition-filtered view of the table
     * without mutating the original.
     *
     * <p>NOTE: keep this manual copy-constructor in sync whenever a new field is added to this
     * class, otherwise the new field will be silently lost in the copy.
     */
    public JdbcSourceTable withQuery(String query) {
        return JdbcSourceTable.builder()
                .tablePath(this.tablePath)
                .query(query)
                .partitionColumn(this.partitionColumn)
                .partitionNumber(this.partitionNumber)
                .partitionStart(this.partitionStart)
                .partitionEnd(this.partitionEnd)
                .useSelectCount(this.useSelectCount)
                .skipAnalyze(this.skipAnalyze)
                .catalogTable(this.catalogTable)
                .build();
    }
}
