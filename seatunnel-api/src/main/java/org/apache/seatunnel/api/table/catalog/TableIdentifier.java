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
import java.util.Objects;

public final class TableIdentifier implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String catalogName;

    private final String databaseName;

    private final String tableName;

    private TableIdentifier(String catalogName, String databaseName, String tableName) {
        this.catalogName = catalogName;
        this.databaseName = databaseName;
        this.tableName = tableName;
    }

    public static TableIdentifier of(String catalogName, String databaseName, String tableName) {
        return new TableIdentifier(catalogName, databaseName, tableName);
    }

    public String getCatalogName() {
        return catalogName;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String gettableName() {
        return tableName;
    }

    public TablePath toTablePath() {
        return TablePath.of(databaseName, tableName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TableIdentifier that = (TableIdentifier) o;
        return catalogName.equals(that.catalogName)
                && databaseName.equals(that.databaseName)
                && tableName.equals(that.tableName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(catalogName, databaseName, tableName);
    }

    @Override
    public String toString() {
        return String.join(".", catalogName, databaseName, tableName);
    }
}
