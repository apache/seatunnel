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

package org.apache.seatunnel.transform.table;

import org.apache.seatunnel.shade.com.fasterxml.jackson.annotation.JsonAlias;
import org.apache.seatunnel.shade.com.google.common.base.Preconditions;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.TablePath;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;

import java.io.Serializable;

@Getter
@Setter
@Accessors(chain = true)
@ToString
public class TableFilterConfig implements Serializable {

    public static final String PLUGIN_NAME = "TableFilter";

    public static final Option<String> DATABASE_PATTERN =
            Options.key("database_pattern")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Specify database filter pattern"
                                    + "The default value is null, which means no filtering. "
                                    + "If you want to filter the database name, please set it to a regular expression.");

    public static final Option<String> SCHEMA_PATTERN =
            Options.key("schema_pattern")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Specify schema filter pattern"
                                    + "The default value is null, which means no filtering. "
                                    + "If you want to filter the schema name, please set it to a regular expression.");

    public static final Option<String> TABLE_PATTERN =
            Options.key("table_pattern")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Specify table filter pattern"
                                    + "The default value is null, which means no filtering. "
                                    + "If you want to filter the table name, please set it to a regular expression.");

    public static final Option<PatternMode> PATTERN_MODE =
            Options.key("pattern_mode")
                    .enumType(PatternMode.class)
                    .defaultValue(PatternMode.INCLUDE)
                    .withDescription(
                            "Specify pattern mode"
                                    + "The default value is INCLUDE, which means include the matched table."
                                    + "If you want to exclude the matched table, please set it to EXCLUDE.");

    @JsonAlias("database_pattern")
    private String databasePattern;

    @JsonAlias("schema_pattern")
    private String schemaPattern;

    @JsonAlias("table_pattern")
    private String tablePattern;

    @JsonAlias("pattern_mode")
    private PatternMode patternMode;

    public boolean isMatch(TablePath tablePath) {
        if (PatternMode.INCLUDE.equals(patternMode)) {
            if (databasePattern != null && !tablePath.getDatabaseName().matches(databasePattern)) {
                return false;
            }
            if (schemaPattern != null && !tablePath.getSchemaName().matches(schemaPattern)) {
                return false;
            }
            if (tablePattern != null && !tablePath.getTableName().matches(tablePattern)) {
                return false;
            }
            return true;
        }

        if (databasePattern != null && tablePath.getDatabaseName().matches(databasePattern)) {
            return false;
        }
        if (schemaPattern != null && tablePath.getSchemaName().matches(schemaPattern)) {
            return false;
        }
        if (tablePattern != null && tablePath.getTableName().matches(tablePattern)) {
            return false;
        }
        return true;
    }

    public static TableFilterConfig of(ReadonlyConfig config) {
        TableFilterConfig filterConfig = new TableFilterConfig();
        filterConfig.setDatabasePattern(config.get(DATABASE_PATTERN));
        filterConfig.setSchemaPattern(config.get(SCHEMA_PATTERN));
        filterConfig.setTablePattern(config.get(TABLE_PATTERN));
        filterConfig.setPatternMode(config.get(PATTERN_MODE));

        Preconditions.checkArgument(
                filterConfig.getDatabasePattern() != null
                        || filterConfig.getSchemaPattern() != null
                        || filterConfig.getTablePattern() != null
                        || filterConfig.getPatternMode() != null,
                "At least one of database_pattern, schema_pattern, table_pattern or pattern_mode must be specified.");
        return filterConfig;
    }

    public enum PatternMode {
        INCLUDE,
        EXCLUDE;
    }
}
