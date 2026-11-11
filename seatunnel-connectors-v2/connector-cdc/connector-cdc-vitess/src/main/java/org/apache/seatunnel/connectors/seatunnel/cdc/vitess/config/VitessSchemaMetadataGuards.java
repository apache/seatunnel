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

package org.apache.seatunnel.connectors.seatunnel.cdc.vitess.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConditionExtension;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.options.table.ColumnOptions;

import java.util.List;
import java.util.Map;

/**
 * Reusable {@link ConditionExtension} guards for the Vitess CDC schema metadata contract.
 *
 * <p>Vitess does not provide a SeaTunnel catalog yet, so a source has to describe the tables it
 * reads explicitly through {@code schema} or {@code tables_configs}, and the declarative rule
 * cannot look inside those structures. Each guard below attaches one of the checks the source
 * applies while the job is built to the option it belongs to, so a configuration that cannot
 * produce deterministic table schemas is rejected when the job is submitted instead of when it
 * starts. The predicate and the wording are shared with the source through {@link
 * #declaresSchemaMetadata(Map)}, {@link #missingSchemaBlockMessage(String)} and {@link
 * #missingMetadataMessage(String)}, so an operator sees the same message either way. The schema
 * guards report that message instead of a plain description because the offending entry has to be
 * named, while the selector guard returns {@code false} and lets the rule render its description.
 *
 * <p>Runtime behavior is unchanged. A configuration accepted here can still fail later when the
 * declared tables are resolved against the source metadata, because that requires reading the
 * source and is intentionally not part of the rule.
 */
public final class VitessSchemaMetadataGuards {

    private VitessSchemaMetadataGuards() {}

    /**
     * Returns whether a schema block declares its columns directly or refers to the metadata table.
     */
    public static boolean declaresSchemaMetadata(Map<String, Object> schema) {
        return schema != null
                && (schema.containsKey(ColumnOptions.COLUMNS.key())
                        || schema.containsKey(ColumnOptions.METADATA_TABLE_ID.key()));
    }

    /** Message for a schema block that declares neither columns nor a metadata table id. */
    public static String missingMetadataMessage(String optionName) {
        return String.format(
                "Vitess CDC requires explicit columns or metadata_table_id in '%s' so table schemas stay deterministic.",
                optionName);
    }

    /** Message for a {@code tables_configs} entry that does not carry a schema block. */
    public static String missingSchemaBlockMessage(String optionName) {
        return String.format(
                "Vitess CDC requires explicit schema metadata. '%s' must contain a schema block.",
                optionName);
    }

    /** Rejects a {@code schema} that declares neither columns nor a metadata table id. */
    public static ConditionExtension<Map<String, Object>> rootSchemaGuard(
            Option<Map<String, Object>> schemaOption) {
        return new RootSchemaGuard(schemaOption.key());
    }

    /**
     * Rejects {@code tables_configs} entries that do not carry a schema block declaring columns or
     * a metadata table id. An empty list is left to the not-empty constraint of the same option.
     */
    public static ConditionExtension<List<Map<String, Object>>> tableConfigsGuard(
            Option<List<Map<String, Object>>> tableConfigsOption) {
        return new TableConfigsGuard(tableConfigsOption.key());
    }

    /** Rejects a configuration that sets both {@code table-names} and {@code table-pattern}. */
    public static ConditionExtension<List<String>> tableNamesGuard(
            Option<String> tablePatternOption) {
        return new TableNamesGuard(tablePatternOption);
    }

    static final class RootSchemaGuard implements ConditionExtension<Map<String, Object>> {

        private final String guardedKey;

        RootSchemaGuard(String guardedKey) {
            this.guardedKey = guardedKey;
        }

        @Override
        public String description() {
            return "must declare '"
                    + ColumnOptions.COLUMNS.key()
                    + "' or '"
                    + ColumnOptions.METADATA_TABLE_ID.key()
                    + "' so table schemas stay deterministic";
        }

        @Override
        public boolean evaluate(ReadonlyConfig config, Map<String, Object> value) {
            if (declaresSchemaMetadata(value)) {
                return true;
            }
            throw new OptionValidationException(missingMetadataMessage(guardedKey));
        }
    }

    static final class TableConfigsGuard implements ConditionExtension<List<Map<String, Object>>> {

        private final String guardedKey;

        TableConfigsGuard(String guardedKey) {
            this.guardedKey = guardedKey;
        }

        @Override
        public String description() {
            return "must contain a '"
                    + ConnectorCommonOptions.SCHEMA.key()
                    + "' block declaring '"
                    + ColumnOptions.COLUMNS.key()
                    + "' or '"
                    + ColumnOptions.METADATA_TABLE_ID.key()
                    + "' for every entry";
        }

        @Override
        @SuppressWarnings("unchecked")
        public boolean evaluate(ReadonlyConfig config, List<Map<String, Object>> value) {
            if (value == null) {
                return true;
            }
            for (int index = 0; index < value.size(); index++) {
                Map<String, Object> tableConfig = value.get(index);
                Object schemaValue =
                        tableConfig == null
                                ? null
                                : tableConfig.get(ConnectorCommonOptions.SCHEMA.key());
                String optionName = guardedKey + "[" + index + "]";
                if (!(schemaValue instanceof Map)) {
                    throw new OptionValidationException(missingSchemaBlockMessage(optionName));
                }
                if (!declaresSchemaMetadata((Map<String, Object>) schemaValue)) {
                    throw new OptionValidationException(
                            missingMetadataMessage(optionName + ".schema"));
                }
            }
            return true;
        }
    }

    static final class TableNamesGuard implements ConditionExtension<List<String>> {

        private final Option<String> tablePatternOption;

        TableNamesGuard(Option<String> tablePatternOption) {
            this.tablePatternOption = tablePatternOption;
        }

        @Override
        public String description() {
            return "must not be used together with '" + tablePatternOption.key() + "'";
        }

        @Override
        public boolean evaluate(ReadonlyConfig config, List<String> value) {
            return !config.getOptional(tablePatternOption).isPresent();
        }
    }
}
