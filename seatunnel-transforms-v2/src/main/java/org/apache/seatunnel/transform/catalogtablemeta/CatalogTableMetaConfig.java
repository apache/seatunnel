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

package org.apache.seatunnel.transform.catalogtablemeta;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.schema.ReadonlyConfigParser;
import org.apache.seatunnel.api.table.catalog.schema.TableSchemaOptions;
import org.apache.seatunnel.api.table.catalog.schema.TableSchemaParser;

import org.apache.commons.lang3.StringUtils;

import lombok.Data;

import java.util.List;
import java.util.Map;

import static org.apache.seatunnel.api.table.catalog.CatalogTableOptions.CATALOG_NAME;
import static org.apache.seatunnel.api.table.catalog.CatalogTableOptions.OPTIONS;
import static org.apache.seatunnel.api.table.catalog.CatalogTableOptions.PRIMARY_KEYS;
import static org.apache.seatunnel.api.table.catalog.schema.TableSchemaOptions.TableIdentifierOptions.COMMENT;

@Data
public class CatalogTableMetaConfig {

    private static final TableSchemaParser.ConstraintKeyParser<ReadonlyConfig> constraintKeyParser =
            new ReadonlyConfigParser.ConstraintKeyParser();
    private static final TableSchemaParser.PrimaryKeyParser<ReadonlyConfig> primaryKeyParser =
            new ReadonlyConfigParser.PrimaryKeyParser();

    public static final Option<List<Map<String, Object>>> MULTI_TABLES =
            Options.key("table_transform")
                    .type(new TypeReference<List<Map<String, Object>>>() {})
                    .noDefaultValue()
                    .withDescription("");

    public static final Option<String> TABLE_PATH =
            Options.key("tablePath").stringType().noDefaultValue().withDescription("");

    private String[] partitionKeys = new String[] {};

    private String tablePath;

    private String comment;

    private String catalogName;

    private Map<String, String> options;

    private PrimaryKey primaryKey;

    private List<ConstraintKey> constraintKeys;

    public static CatalogTableMetaConfig of(ReadonlyConfig config) {

        CatalogTableMetaConfig catalogTableMetaConfig = new CatalogTableMetaConfig();
        if (config.getOptional(PRIMARY_KEYS).isPresent()) {
            catalogTableMetaConfig.setPartitionKeys(
                    config.get(PRIMARY_KEYS).toArray(new String[0]));
        }
        catalogTableMetaConfig.setComment(config.get(COMMENT));
        catalogTableMetaConfig.setCatalogName(config.get(CATALOG_NAME));
        catalogTableMetaConfig.setOptions(config.get(OPTIONS));
        if (config.getOptional(TableSchemaOptions.PrimaryKeyOptions.PRIMARY_KEY).isPresent()) {
            catalogTableMetaConfig.setPrimaryKey(primaryKeyParser.parse(config));
        }
        if (config.getOptional(TableSchemaOptions.ConstraintKeyOptions.CONSTRAINT_KEYS)
                .isPresent()) {
            catalogTableMetaConfig.setConstraintKeys(constraintKeyParser.parse(config));
        }
        if (StringUtils.isNotEmpty(config.get(TableSchemaOptions.TableIdentifierOptions.TABLE))) {
            catalogTableMetaConfig.setTablePath(
                    config.get(TableSchemaOptions.TableIdentifierOptions.TABLE));
        }
        return catalogTableMetaConfig;
    }

    public static CatalogTableMetaConfig of(ReadonlyConfig config, CatalogTable catalogTable) {
        String tablePath = catalogTable.getTableId().toTablePath().getFullName();
        if (null != config.get(MULTI_TABLES)) {
            return config.get(MULTI_TABLES).stream()
                    .map(ReadonlyConfig::fromMap)
                    .filter(catalogtableConf -> tablePath.equals(catalogtableConf.get(TABLE_PATH)))
                    .map(CatalogTableMetaConfig::of)
                    .findFirst()
                    .orElseGet(() -> of(config));
        }
        return of(config);
    }
}
