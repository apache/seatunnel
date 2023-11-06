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

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;

import java.util.List;
import java.util.Map;

public class TableSchemaOptions {

    public static class TableIdentifierOptions {

        public static final Option<Boolean> SCHEMA_FIRST =
                Options.key("schema_first")
                        .booleanType()
                        .defaultValue(false)
                        .withDescription("Parse Schema First from table");

        public static final Option<String> TABLE =
                Options.key("table")
                        .stringType()
                        .noDefaultValue()
                        .withDescription("SeaTunnel Schema Full Table Name");

        public static final Option<String> COMMENT =
                Options.key("comment")
                        .stringType()
                        .noDefaultValue()
                        .withDescription("SeaTunnel Schema Table Comment");
    }

    public static final Option<Map<String, Object>> SCHEMA =
            Options.key("schema")
                    .type(new TypeReference<Map<String, Object>>() {})
                    .noDefaultValue()
                    .withDescription("SeaTunnel Schema");

    // We should use ColumnOptions instead of FieldOptions
    @Deprecated
    public static class FieldOptions {
        public static final Option<Map<String, Object>> FIELDS =
                Options.key("schema.fields")
                        .type(new TypeReference<Map<String, Object>>() {})
                        .noDefaultValue()
                        .withDescription("SeaTunnel Schema Fields");
    }

    public static class ColumnOptions {

        // todo: how to define List<Map<String, Object>>
        public static final Option<List<Map<String, Object>>> COLUMNS =
                Options.key("columns")
                        .type(new TypeReference<List<Map<String, Object>>>() {})
                        .noDefaultValue()
                        .withDescription("SeaTunnel Schema Columns");

        public static final Option<String> NAME =
                Options.key("name")
                        .stringType()
                        .noDefaultValue()
                        .withDescription("SeaTunnel Schema Column Name");

        public static final Option<String> TYPE =
                Options.key("type")
                        .stringType()
                        .noDefaultValue()
                        .withDescription("SeaTunnel Schema Column Type");

        public static final Option<Integer> COLUMN_LENGTH =
                Options.key("columnLength")
                        .intType()
                        .defaultValue(0)
                        .withDescription("SeaTunnel Schema Column Length");

        public static final Option<Boolean> NULLABLE =
                Options.key("nullable")
                        .booleanType()
                        .defaultValue(true)
                        .withDescription("SeaTunnel Schema Column Nullable");

        public static final Option<Object> DEFAULT_VALUE =
                Options.key("defaultValue")
                        .objectType(Object.class)
                        .noDefaultValue()
                        .withDescription("SeaTunnel Schema Column Default Value");

        public static final Option<String> COMMENT =
                Options.key("comment")
                        .stringType()
                        .noDefaultValue()
                        .withDescription("SeaTunnel Schema Column Comment");
    }

    public static class PrimaryKeyOptions {

        public static final Option<Map<String, Object>> PRIMARY_KEY =
                Options.key("primaryKey")
                        .type(new TypeReference<Map<String, Object>>() {})
                        .noDefaultValue()
                        .withDescription("SeaTunnel Schema Fields");

        public static final Option<String> PRIMARY_KEY_NAME =
                Options.key("name")
                        .stringType()
                        .noDefaultValue()
                        .withDescription("SeaTunnel Schema Primary Key Name");

        public static final Option<List<String>> PRIMARY_KEY_COLUMNS =
                Options.key("columnNames")
                        .listType()
                        .noDefaultValue()
                        .withDescription("SeaTunnel Schema Primary Key Columns");
    }

    public static class ConstraintKeyOptions {
        public static final Option<List<Map<String, Object>>> CONSTRAINT_KEYS =
                Options.key("constraintKeys")
                        .type(new TypeReference<List<Map<String, Object>>>() {})
                        .noDefaultValue()
                        .withDescription(
                                "SeaTunnel Schema Constraint Keys. e.g. [{name: \"xx_index\", type: \"KEY\", columnKeys: [{columnName: \"name\", sortType: \"ASC\"}]}]");

        public static final Option<String> CONSTRAINT_KEY_NAME =
                Options.key("constraintName")
                        .stringType()
                        .noDefaultValue()
                        .withDescription("SeaTunnel Schema Constraint Key Name");

        public static final Option<ConstraintKey.ConstraintType> CONSTRAINT_KEY_TYPE =
                Options.key("constraintType")
                        .enumType(ConstraintKey.ConstraintType.class)
                        .noDefaultValue()
                        .withDescription(
                                "SeaTunnel Schema Constraint Key Type, e.g. KEY, UNIQUE_KEY, FOREIGN_KEY");

        public static final Option<List<Map<String, Object>>> CONSTRAINT_KEY_COLUMNS =
                Options.key("constraintColumns")
                        .type(new TypeReference<List<Map<String, Object>>>() {})
                        .noDefaultValue()
                        .withDescription(
                                "SeaTunnel Schema Constraint Key Columns. e.g. [{columnName: \"name\", sortType: \"ASC\"}]");

        public static final Option<String> CONSTRAINT_KEY_COLUMN_NAME =
                Options.key("columnName")
                        .stringType()
                        .noDefaultValue()
                        .withDescription("SeaTunnel Schema Constraint Key Column Name");

        public static final Option<ConstraintKey.ColumnSortType> CONSTRAINT_KEY_COLUMN_SORT_TYPE =
                Options.key("sortType")
                        .enumType(ConstraintKey.ColumnSortType.class)
                        .defaultValue(ConstraintKey.ColumnSortType.ASC)
                        .withDescription(
                                "SeaTunnel Schema Constraint Key Column Sort Type, e.g. ASC, DESC");
    }
}
