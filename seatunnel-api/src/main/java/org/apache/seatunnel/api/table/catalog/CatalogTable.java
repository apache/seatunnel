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

package org.apache.seatunnel.api.table.catalog;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/** Represent the table metadata in SeaTunnel. */
public final class CatalogTable implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Used to identify the table. */
    private final TableIdentifier tableId;

    /** The table schema metadata. */
    private final TableSchema tableSchema;

    private final Map<String, String> options;

    private final List<String> partitionKeys;

    private final String comment;

    private final String catalogName;

    public static CatalogTable of(
            TableIdentifier tableId,
            TableSchema tableSchema,
            Map<String, String> options,
            List<String> partitionKeys,
            String comment) {
        return new CatalogTable(tableId, tableSchema, options, partitionKeys, comment);
    }

    public static CatalogTable of(
            TableIdentifier tableId,
            TableSchema tableSchema,
            Map<String, String> options,
            List<String> partitionKeys,
            String comment,
            String catalogName) {
        return new CatalogTable(tableId, tableSchema, options, partitionKeys, comment, catalogName);
    }

    private CatalogTable(
            TableIdentifier tableId,
            TableSchema tableSchema,
            Map<String, String> options,
            List<String> partitionKeys,
            String comment) {
        this(tableId, tableSchema, options, partitionKeys, comment, "");
    }

    private CatalogTable(
            TableIdentifier tableId,
            TableSchema tableSchema,
            Map<String, String> options,
            List<String> partitionKeys,
            String comment,
            String catalogName) {
        this.tableId = tableId;
        this.tableSchema = tableSchema;
        this.options = options;
        this.partitionKeys = partitionKeys;
        this.comment = comment;
        this.catalogName = catalogName;
    }

    public TableIdentifier getTableId() {
        return tableId;
    }

    public TableSchema getTableSchema() {
        return tableSchema;
    }

    public Map<String, String> getOptions() {
        return options;
    }

    public List<String> getPartitionKeys() {
        return partitionKeys;
    }

    public String getComment() {
        return comment;
    }

    public String getCatalogName() {
        return catalogName;
    }

    @Override
    public String toString() {
        return "CatalogTable{"
                + "tableId="
                + tableId
                + ", tableSchema="
                + tableSchema
                + ", options="
                + options
                + ", partitionKeys="
                + partitionKeys
                + ", comment='"
                + comment
                + '\''
                + '}';
    }
}
