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

package org.apache.seatunnel.api.table.catalog.schema;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
public class CatalogTableParser {

    private final String catalogName;

    public CatalogTableParser(String catalogName) {
        this.catalogName = catalogName;
    }

    /**
     * Parse CatalogTables from readonlyConfig. The readonlyConfig must contain schema or schemas.
     *
     * @param readonlyConfig readonlyConfig
     * @return CatalogTables
     */
    public List<CatalogTable> parse(ReadonlyConfig readonlyConfig) {
        Optional<Map<String, Object>> schemaOption =
                readonlyConfig.getOptional(TableSchemaOptions.SCHEMA);
        Optional<List<Map<String, Object>>> schemasOption =
                readonlyConfig.getOptional(TableSchemaOptions.SCHEMAS);

        if (schemaOption.isPresent() && schemasOption.isPresent()) {
            throw new IllegalArgumentException("schema and schemas can't be set at the same time");
        }

        if (schemaOption.isPresent()) {
            ReadonlyConfig schemaConfig = schemaOption.map(ReadonlyConfig::fromMap).get();
            return Lists.newArrayList(parseFromSchema(schemaConfig));
        }

        if (schemasOption.isPresent()) {
            return schemasOption.get().stream()
                    .map(ReadonlyConfig::fromMap)
                    .map(this::parseFromSchema)
                    .collect(Collectors.toList());
        }

        // todo: Should remove this after SCHEMA_FIELDS has removed.
        if (readonlyConfig.getOptional(TableSchemaOptions.FieldOptions.SCHEMA_FIELDS).isPresent()) {
            log.warn("The schema.fields will be deprecated, please use schema instead");
            return Lists.newArrayList(parseFromSchemaFields(readonlyConfig));
        }

        throw new IllegalArgumentException("Schema or Schemas must be set");
    }

    @Deprecated
    private CatalogTable parseFromSchemaFields(ReadonlyConfig readonlyConfig) {
        TableSchema.Builder tableSchemaBuilder = TableSchema.builder();
        tableSchemaBuilder.columns(new TableSchemaParser.FieldParser().parse(readonlyConfig));
        TableSchema tableSchema = tableSchemaBuilder.build();

        return CatalogTable.of(
                TableIdentifier.of(catalogName, "", ""),
                tableSchema,
                new HashMap<>(),
                // todo: add partitionKeys?
                new ArrayList<>(),
                readonlyConfig.get(TableSchemaOptions.TableIdentifierOptions.COMMENT));
    }

    private CatalogTable parseFromSchema(ReadonlyConfig schemaConfig) {

        TableSchema tableSchema = new TableSchemaParser().parse(schemaConfig);
        TablePath tablePath = parseTablePath(schemaConfig);
        return CatalogTable.of(
                TableIdentifier.of(catalogName, tablePath),
                tableSchema,
                new HashMap<>(),
                // todo: add partitionKeys?
                new ArrayList<>(),
                schemaConfig.get(TableSchemaOptions.TableIdentifierOptions.COMMENT));
    }

    private TablePath parseTablePath(ReadonlyConfig schemaConfig) {
        if (!schemaConfig
                .getOptional(TableSchemaOptions.TableIdentifierOptions.TABLE)
                .isPresent()) {
            return TablePath.EMPTY;
        }
        return TablePath.of(
                schemaConfig.get(TableSchemaOptions.TableIdentifierOptions.TABLE),
                schemaConfig.get(TableSchemaOptions.TableIdentifierOptions.SCHEMA_FIRST));
    }
}
