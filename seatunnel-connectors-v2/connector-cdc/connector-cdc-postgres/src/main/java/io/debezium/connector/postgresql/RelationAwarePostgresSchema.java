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

package io.debezium.connector.postgresql;

import io.debezium.connector.postgresql.connection.PostgresDefaultValueConverter;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;

import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

/** A PostgreSQL schema that exposes pgoutput RELATION changes to SeaTunnel. */
public class RelationAwarePostgresSchema extends PostgresSchema {

    private Consumer<Table> relationChangeListener;

    public RelationAwarePostgresSchema(
            PostgresConnectorConfig config,
            TypeRegistry typeRegistry,
            PostgresDefaultValueConverter defaultValueConverter,
            TopicSelector<TableId> topicSelector,
            PostgresValueConverter valueConverter) {
        super(config, typeRegistry, defaultValueConverter, topicSelector, valueConverter);
    }

    public void setRelationChangeListener(Consumer<Table> relationChangeListener) {
        this.relationChangeListener = relationChangeListener;
    }

    /**
     * Updates Debezium's schema first and then publishes a synthetic schema record before the DML
     * record that caused PostgreSQL to send the RELATION message.
     */
    @Override
    public void applySchemaChangesForTable(int relationId, Table table) {
        Table previousTable = tableFor(table.id());
        super.applySchemaChangesForTable(relationId, table);

        // Always forward streaming RELATION messages for known tables. After recovery Debezium may
        // have refreshed its in-memory schema from the live database already, while SeaTunnel's
        // restored CatalogTable still contains the schema from the checkpoint. Comparing only with
        // Debezium's current table would then hide the change from SeaTunnel.
        if (relationChangeListener != null && previousTable != null) {
            relationChangeListener.accept(table);
        }
    }

    /** Compare two raw pgoutput RELATION schemas, including their primary-key definitions. */
    public static boolean hasSameRelationSchema(Table before, Table after) {
        if (!Objects.equals(before.primaryKeyColumnNames(), after.primaryKeyColumnNames())) {
            return false;
        }

        List<Column> beforeColumns = before.columns();
        List<Column> afterColumns = after.columns();
        if (beforeColumns.size() != afterColumns.size()) {
            return false;
        }

        for (int i = 0; i < beforeColumns.size(); i++) {
            if (!hasSameColumnDefinition(beforeColumns.get(i), afterColumns.get(i))) {
                return false;
            }
        }
        return true;
    }

    private static boolean hasSameColumnDefinition(Column before, Column after) {
        return Objects.equals(before.name(), after.name())
                && before.jdbcType() == after.jdbcType()
                && before.nativeType() == after.nativeType()
                && Objects.equals(before.typeName(), after.typeName())
                && Objects.equals(before.typeExpression(), after.typeExpression())
                && before.length() == after.length()
                && Objects.equals(before.scale(), after.scale())
                && before.isOptional() == after.isOptional();
    }
}
