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

package org.apache.seatunnel.api.options.table;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;

import java.util.List;
import java.util.Map;

public interface ConstraintKeyOptions {

    Option<List<Map<String, Object>>> CONSTRAINT_KEYS =
            Options.key("constraintKeys")
                    .type(new TypeReference<List<Map<String, Object>>>() {})
                    .noDefaultValue()
                    .withDescription(
                            "SeaTunnel Schema Constraint Keys. e.g. [{name: \"xx_index\", type: \"KEY\", columnKeys: [{columnName: \"name\", sortType: \"ASC\"}]}]");

    Option<String> CONSTRAINT_KEY_NAME =
            Options.key("constraintName")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("SeaTunnel Schema Constraint Key Name");

    Option<ConstraintKey.ConstraintType> CONSTRAINT_KEY_TYPE =
            Options.key("constraintType")
                    .enumType(ConstraintKey.ConstraintType.class)
                    .noDefaultValue()
                    .withDescription(
                            "SeaTunnel Schema Constraint Key Type, e.g. KEY, UNIQUE_KEY, FOREIGN_KEY");

    Option<List<Map<String, Object>>> CONSTRAINT_KEY_COLUMNS =
            Options.key("constraintColumns")
                    .type(new TypeReference<List<Map<String, Object>>>() {})
                    .noDefaultValue()
                    .withDescription(
                            "SeaTunnel Schema Constraint Key Columns. e.g. [{columnName: \"name\", sortType: \"ASC\"}]");

    Option<String> CONSTRAINT_KEY_COLUMN_NAME =
            Options.key("columnName")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("SeaTunnel Schema Constraint Key Column Name");

    Option<ConstraintKey.ColumnSortType> CONSTRAINT_KEY_COLUMN_SORT_TYPE =
            Options.key("sortType")
                    .enumType(ConstraintKey.ColumnSortType.class)
                    .defaultValue(ConstraintKey.ColumnSortType.ASC)
                    .withDescription(
                            "SeaTunnel Schema Constraint Key Column Sort Type, e.g. ASC, DESC");
}
