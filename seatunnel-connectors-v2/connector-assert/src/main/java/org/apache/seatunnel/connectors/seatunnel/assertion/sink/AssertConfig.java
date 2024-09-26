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

package org.apache.seatunnel.connectors.seatunnel.assertion.sink;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.List;
import java.util.Map;

public class AssertConfig {

    public static final String RULE_TYPE = "rule_type";

    public static final String RULE_VALUE = "rule_value";

    public static final String EQUALS_TO = "equals_to";

    public static final String ROW_RULES = "row_rules";

    public static final String FIELD_NAME = "field_name";

    public static final String FIELD_TYPE = "field_type";

    public static final String FIELD_VALUE = "field_value";

    public static final String FIELD_RULES = "field_rules";

    public static final String CATALOG_TABLE_RULES = "catalog_table_rule";

    public static final String PRIMARY_KEY_RULE = "primary_key_rule";
    public static final String PRIMARY_KEY_NAME = "primary_key_name";
    public static final String PRIMARY_KEY_COLUMNS = "primary_key_columns";

    public static final String CONSTRAINT_KEY_RULE = "constraint_key_rule";
    public static final String CONSTRAINT_KEY_NAME = "constraint_key_name";
    public static final String CONSTRAINT_KEY_TYPE = "constraint_key_type";
    public static final String CONSTRAINT_KEY_COLUMNS = "constraint_key_columns";
    public static final String CONSTRAINT_KEY_COLUMN_NAME = "constraint_key_column_name";
    public static final String CONSTRAINT_KEY_SORT_TYPE = "constraint_key_sort_type";

    public static final String COLUMN_RULE = "column_rule";

    public static final String COLUMN_NAME = "name";
    public static final String COLUMN_TYPE = "type";
    public static final String COLUMN_LENGTH = "column_length";
    public static final String COLUMN_NULLABLE = "nullable";
    public static final String COLUMN_DEFAULT_VALUE = "default_value";
    public static final String COLUMN_COMMENT = "comment";

    public static class TableIdentifierRule {
        public static final String TABLE_IDENTIFIER_RULE = "table_identifier_rule";

        public static final String TABLE_IDENTIFIER_CATALOG_NAME = "catalog_name";
        public static final String TABLE_IDENTIFIER_TABLE_NAME = "table";
    }

    public static final Option<String> COMMENT =
            Options.key("comment")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("SeaTunnel Schema Column Comment");

    public static final Option<Map<String, Object>> RULES =
            Options.key("rules")
                    .type(new TypeReference<Map<String, Object>>() {})
                    .noDefaultValue()
                    .withDescription(
                            "Rule definition of user's available data. Each rule represents one field validation or row num validation.");

    public static final Option<List<Map<String, Object>>> TABLE_CONFIGS =
            Options.key("tables_configs")
                    .type(new TypeReference<List<Map<String, Object>>>() {})
                    .noDefaultValue()
                    .withDescription(
                            "Table configuration for the sink. Each table configuration contains the table name and the rules for the table.");

    public static final Option<String> TABLE_PATH =
            Options.key("table_path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("table full path");
}
