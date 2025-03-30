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

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.SeaTunnelDataTypeConvertorUtil;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.common.config.TypesafeConfigUtils;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.seatunnel.connectors.seatunnel.assertion.sink.AssertConfig.COLUMN_COMMENT;
import static org.apache.seatunnel.connectors.seatunnel.assertion.sink.AssertConfig.COLUMN_DEFAULT_VALUE;
import static org.apache.seatunnel.connectors.seatunnel.assertion.sink.AssertConfig.COLUMN_LENGTH;
import static org.apache.seatunnel.connectors.seatunnel.assertion.sink.AssertConfig.COLUMN_NAME;
import static org.apache.seatunnel.connectors.seatunnel.assertion.sink.AssertConfig.COLUMN_NULLABLE;
import static org.apache.seatunnel.connectors.seatunnel.assertion.sink.AssertConfig.COLUMN_RULE;
import static org.apache.seatunnel.connectors.seatunnel.assertion.sink.AssertConfig.COLUMN_TYPE;
import static org.apache.seatunnel.connectors.seatunnel.assertion.sink.AssertConfig.CONSTRAINT_KEY_COLUMNS;
import static org.apache.seatunnel.connectors.seatunnel.assertion.sink.AssertConfig.CONSTRAINT_KEY_COLUMN_NAME;
import static org.apache.seatunnel.connectors.seatunnel.assertion.sink.AssertConfig.CONSTRAINT_KEY_NAME;
import static org.apache.seatunnel.connectors.seatunnel.assertion.sink.AssertConfig.CONSTRAINT_KEY_RULE;
import static org.apache.seatunnel.connectors.seatunnel.assertion.sink.AssertConfig.CONSTRAINT_KEY_SORT_TYPE;
import static org.apache.seatunnel.connectors.seatunnel.assertion.sink.AssertConfig.CONSTRAINT_KEY_TYPE;
import static org.apache.seatunnel.connectors.seatunnel.assertion.sink.AssertConfig.PRIMARY_KEY_COLUMNS;
import static org.apache.seatunnel.connectors.seatunnel.assertion.sink.AssertConfig.PRIMARY_KEY_NAME;
import static org.apache.seatunnel.connectors.seatunnel.assertion.sink.AssertConfig.PRIMARY_KEY_RULE;
import static org.apache.seatunnel.connectors.seatunnel.assertion.sink.AssertConfig.TableIdentifierRule.TABLE_IDENTIFIER_CATALOG_NAME;
import static org.apache.seatunnel.connectors.seatunnel.assertion.sink.AssertConfig.TableIdentifierRule.TABLE_IDENTIFIER_RULE;
import static org.apache.seatunnel.connectors.seatunnel.assertion.sink.AssertConfig.TableIdentifierRule.TABLE_IDENTIFIER_TABLE_NAME;

public class AssertCatalogTableRuleParser {

    public AssertCatalogTableRule parseCatalogTableRule(Config catalogTableRule) {
        AssertCatalogTableRule tableRule = new AssertCatalogTableRule();

        parsePrimaryKeyRule(catalogTableRule).ifPresent(tableRule::setPrimaryKeyRule);
        parseConstraintKeyRule(catalogTableRule).ifPresent(tableRule::setConstraintKeyRule);
        parseColumnRule(catalogTableRule).ifPresent(tableRule::setColumnRule);
        parseTableIdentifierRule(catalogTableRule).ifPresent(tableRule::setTableIdentifierRule);
        return tableRule;
    }

    private Optional<AssertCatalogTableRule.AssertPrimaryKeyRule> parsePrimaryKeyRule(
            Config catalogTableRule) {
        if (!catalogTableRule.hasPath(PRIMARY_KEY_RULE)) {
            return Optional.empty();
        }
        Config primaryKey = catalogTableRule.getConfig(PRIMARY_KEY_RULE);
        return Optional.of(
                new AssertCatalogTableRule.AssertPrimaryKeyRule(
                        primaryKey.getString(PRIMARY_KEY_NAME),
                        primaryKey.getStringList(PRIMARY_KEY_COLUMNS)));
    }

    private Optional<AssertCatalogTableRule.AssertColumnRule> parseColumnRule(
            Config catalogTableRule) {
        if (!catalogTableRule.hasPath(COLUMN_RULE)) {
            return Optional.empty();
        }
        List<Column> columns =
                catalogTableRule.getConfigList(COLUMN_RULE).stream()
                        .map(
                                config -> {
                                    String name = config.getString(COLUMN_NAME);
                                    String type = config.getString(COLUMN_TYPE);
                                    Long columnLength =
                                            TypesafeConfigUtils.getConfig(
                                                    config,
                                                    COLUMN_LENGTH,
                                                    ConnectorCommonOptions.COLUMN_LENGTH
                                                            .defaultValue());
                                    Boolean nullable =
                                            TypesafeConfigUtils.getConfig(
                                                    config,
                                                    COLUMN_NULLABLE,
                                                    ConnectorCommonOptions.NULLABLE.defaultValue());
                                    Object object =
                                            TypesafeConfigUtils.getConfig(
                                                    config,
                                                    COLUMN_DEFAULT_VALUE,
                                                    ConnectorCommonOptions.DEFAULT_VALUE
                                                            .defaultValue());
                                    String comment =
                                            TypesafeConfigUtils.getConfig(
                                                    config,
                                                    COLUMN_COMMENT,
                                                    ConnectorCommonOptions.COLUMN_COMMENT
                                                            .defaultValue());
                                    return PhysicalColumn.of(
                                            name,
                                            SeaTunnelDataTypeConvertorUtil
                                                    .deserializeSeaTunnelDataType(name, type),
                                            columnLength,
                                            nullable,
                                            object,
                                            comment);
                                })
                        .collect(Collectors.toList());
        return Optional.of(new AssertCatalogTableRule.AssertColumnRule(columns));
    }

    private Optional<AssertCatalogTableRule.AssertConstraintKeyRule> parseConstraintKeyRule(
            Config catalogTableRule) {
        if (!catalogTableRule.hasPath(CONSTRAINT_KEY_RULE)) {
            return Optional.empty();
        }
        List<? extends Config> constraintKey = catalogTableRule.getConfigList(CONSTRAINT_KEY_RULE);
        List<ConstraintKey> constraintKeys =
                constraintKey.stream()
                        .map(
                                config -> {
                                    ConstraintKey.ConstraintType constraintType =
                                            ConstraintKey.ConstraintType.valueOf(
                                                    config.getString(CONSTRAINT_KEY_TYPE));
                                    String constraintKeyName =
                                            config.getString(CONSTRAINT_KEY_NAME);
                                    List<ConstraintKey.ConstraintKeyColumn> constraintKeyColumns =
                                            config.getConfigList(CONSTRAINT_KEY_COLUMNS).stream()
                                                    .map(
                                                            c ->
                                                                    ConstraintKey
                                                                            .ConstraintKeyColumn.of(
                                                                            c.getString(
                                                                                    CONSTRAINT_KEY_COLUMN_NAME),
                                                                            ConstraintKey
                                                                                    .ColumnSortType
                                                                                    .valueOf(
                                                                                            c
                                                                                                    .getString(
                                                                                                            CONSTRAINT_KEY_SORT_TYPE))))
                                                    .collect(Collectors.toList());
                                    return ConstraintKey.of(
                                            constraintType,
                                            constraintKeyName,
                                            constraintKeyColumns);
                                })
                        .collect(Collectors.toList());
        return Optional.of(new AssertCatalogTableRule.AssertConstraintKeyRule(constraintKeys));
    }

    private Optional<AssertCatalogTableRule.AssertTableIdentifierRule> parseTableIdentifierRule(
            Config catalogTableRule) {
        if (!catalogTableRule.hasPath(TABLE_IDENTIFIER_RULE)) {
            return Optional.empty();
        }
        Config tableIdentifierRule = catalogTableRule.getConfig(TABLE_IDENTIFIER_RULE);
        TableIdentifier tableIdentifier =
                TableIdentifier.of(
                        tableIdentifierRule.getString(TABLE_IDENTIFIER_CATALOG_NAME),
                        TablePath.of(tableIdentifierRule.getString(TABLE_IDENTIFIER_TABLE_NAME)));
        return Optional.of(new AssertCatalogTableRule.AssertTableIdentifierRule(tableIdentifier));
    }
}
