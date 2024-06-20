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

package org.apache.seatunnel.connectors.seatunnel.assertion.rule;

import org.apache.seatunnel.api.configuration.util.OptionMark;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.connectors.seatunnel.assertion.exception.AssertConnectorException;

import org.apache.commons.collections4.CollectionUtils;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

import static org.apache.seatunnel.connectors.seatunnel.assertion.exception.AssertConnectorErrorCode.CATALOG_TABLE_FAILED;

@Data
public class AssertCatalogTableRule implements Serializable {

    @OptionMark(description = "assert primary key rule")
    private AssertPrimaryKeyRule primaryKeyRule;

    @OptionMark(description = "constraint key rule")
    private AssertConstraintKeyRule constraintKeyRule;

    @OptionMark(description = "column rule")
    private AssertColumnRule columnRule;

    @OptionMark(description = "tableIdentifier rule")
    private AssertTableIdentifierRule tableIdentifierRule;

    public void checkRule(CatalogTable catalogTable) {
        TableSchema tableSchema = catalogTable.getTableSchema();
        if (tableSchema == null) {
            throw new AssertConnectorException(CATALOG_TABLE_FAILED, "tableSchema is null");
        }
        if (primaryKeyRule != null) {
            primaryKeyRule.checkRule(tableSchema.getPrimaryKey());
        }
        if (constraintKeyRule != null) {
            constraintKeyRule.checkRule(tableSchema.getConstraintKeys());
        }
        if (columnRule != null) {
            columnRule.checkRule(tableSchema.getColumns());
        }
        if (tableIdentifierRule != null) {
            tableIdentifierRule.checkRule(catalogTable.getTableId());
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class AssertPrimaryKeyRule implements Serializable {
        private static final long serialVersionUID = 1L;

        @OptionMark(description = "primary key name")
        private String primaryKeyName;

        @OptionMark(description = "primary key columns")
        private List<String> primaryKeyColumns;

        public void checkRule(PrimaryKey check) {
            if (check == null) {
                throw new AssertConnectorException(CATALOG_TABLE_FAILED, "primaryKey is null");
            }
            if (primaryKeyName != null && !primaryKeyName.equals(check.getPrimaryKey())) {
                throw new AssertConnectorException(
                        CATALOG_TABLE_FAILED,
                        String.format(
                                "primaryKey: %s is not equal to %s",
                                check.getPrimaryKey(), primaryKeyName));
            }
            if (CollectionUtils.isNotEmpty(primaryKeyColumns)
                    && !CollectionUtils.isEqualCollection(
                            primaryKeyColumns, check.getColumnNames())) {
                throw new AssertConnectorException(
                        CATALOG_TABLE_FAILED,
                        String.format(
                                "primaryKey columns: %s is not equal to %s",
                                check.getColumnNames(), primaryKeyColumns));
            }
        }
    }

    @Data
    @AllArgsConstructor
    public static class AssertConstraintKeyRule implements Serializable {
        private static final long serialVersionUID = 1L;
        private List<ConstraintKey> constraintKeys;

        public void checkRule(List<ConstraintKey> check) {
            if (CollectionUtils.isEmpty(check)) {
                throw new AssertConnectorException(CATALOG_TABLE_FAILED, "constraintKeys is null");
            }
            if (CollectionUtils.isNotEmpty(constraintKeys)
                    && !CollectionUtils.isEqualCollection(constraintKeys, check)) {
                throw new AssertConnectorException(
                        CATALOG_TABLE_FAILED,
                        String.format(
                                "constraintKeys: %s is not equal to %s", check, constraintKeys));
            }
        }
    }

    @Data
    @AllArgsConstructor
    public static class AssertColumnRule implements Serializable {
        private static final long serialVersionUID = 1L;

        private List<Column> columns;

        public void checkRule(List<Column> check) {
            if (CollectionUtils.isEmpty(check)) {
                throw new AssertConnectorException(CATALOG_TABLE_FAILED, "columns is null");
            }
            if (CollectionUtils.isNotEmpty(columns)
                    && !CollectionUtils.isEqualCollection(columns, check)) {
                throw new AssertConnectorException(
                        CATALOG_TABLE_FAILED,
                        String.format("columns: %s is not equal to %s", check, columns));
            }
        }
    }

    @Data
    @AllArgsConstructor
    public static class AssertTableIdentifierRule implements Serializable {

        private TableIdentifier tableIdentifier;

        public void checkRule(TableIdentifier actiualTableIdentifier) {
            if (actiualTableIdentifier == null) {
                throw new AssertConnectorException(CATALOG_TABLE_FAILED, "tableIdentifier is null");
            }
            if (!actiualTableIdentifier.equals(tableIdentifier)) {
                throw new AssertConnectorException(
                        CATALOG_TABLE_FAILED,
                        String.format(
                                "tableIdentifier: %s is not equal to %s",
                                actiualTableIdentifier, tableIdentifier));
            }
        }
    }
}
