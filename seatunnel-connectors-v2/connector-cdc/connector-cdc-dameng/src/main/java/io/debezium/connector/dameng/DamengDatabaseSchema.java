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

package io.debezium.connector.dameng;

import io.debezium.relational.HistorizedRelationalDatabaseSchema;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchemaBuilder;
import io.debezium.relational.ValueConverterProvider;
import io.debezium.relational.ddl.DdlParser;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.schema.TopicSelector;
import io.debezium.util.SchemaNameAdjuster;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DamengDatabaseSchema extends HistorizedRelationalDatabaseSchema {
    private final ValueConverterProvider valueConverter;

    public DamengDatabaseSchema(DamengConnectorConfig connectorConfig,
                                ValueConverterProvider valueConverter,
                                TopicSelector<TableId> topicSelector,
                                SchemaNameAdjuster schemaNameAdjuster,
                                boolean tableIdCaseInsensitive) {
        super(connectorConfig,
            topicSelector,
            connectorConfig.getTableFilters().dataCollectionFilter(),
            connectorConfig.getColumnFilter(),
            new TableSchemaBuilder(
                valueConverter,
                schemaNameAdjuster,
                connectorConfig.customConverterRegistry(),
                connectorConfig.getSourceInfoStructMaker().schema(),
                connectorConfig.getSanitizeFieldNames()),
            tableIdCaseInsensitive,
            connectorConfig.getKeyMapper());
        this.valueConverter = valueConverter;
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent schemaChange) {
        log.debug("Applying schema change event {}", schemaChange);

        switch (schemaChange.getType()) {
            case CREATE:
            case ALTER:
                schemaChange.getTableChanges().forEach(x -> {
                    buildAndRegisterSchema(x.getTable());
                    tables().overwriteTable(x.getTable());
                });
                break;
            case DROP:
                schemaChange.getTableChanges().forEach(x -> removeSchema(x.getId()));
                break;
            default:
        }

        if (schemaChange.getTables().stream().map(Table::id).anyMatch(getTableFilter()::isIncluded)) {
            log.debug("Recorded DDL statements for database '{}': {}", schemaChange.getDatabase(), schemaChange.getDdl());
            record(schemaChange, schemaChange.getTableChanges());
        }
    }

    @Override
    protected DdlParser getDdlParser() {
        return null;
    }
}
