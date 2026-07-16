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

package org.apache.seatunnel.connectors.seatunnel.hugegraph.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.List;

public class HugeGraphSinkOptions {

    public static final Option<List<MappingConfig>> MAPPINGS =
            Options.key("mappings")
                    .listType(MappingConfig.class)
                    .noDefaultValue()
                    .withDescription(
                            "List of mapping configurations. Each mapping describes how to write "
                                    + "a single vertex or edge label from the input row.");

    public static final Option<HugeGraphSchemaSaveMode> SCHEMA_SAVE_MODE =
            Options.key("schema_save_mode")
                    .enumType(HugeGraphSchemaSaveMode.class)
                    .defaultValue(HugeGraphSchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST)
                    .withDescription(
                            "Schema management mode. CREATE_SCHEMA_WHEN_NOT_EXIST (default) auto-creates "
                                    + "missing PropertyKey/VertexLabel/EdgeLabel. ERROR_WHEN_SCHEMA_NOT_EXIST "
                                    + "fails if schema is missing.");

    public static final Option<HugeGraphDataSaveMode> DATA_SAVE_MODE =
            Options.key("data_save_mode")
                    .enumType(HugeGraphDataSaveMode.class)
                    .defaultValue(HugeGraphDataSaveMode.APPEND_DATA)
                    .withDescription(
                            "How pre-existing data is handled before writing. APPEND_DATA (default) "
                                    + "keeps existing data. DROP_DATA clears the ENTIRE target graph "
                                    + "(all labels' data and schema) once at job start via the "
                                    + "HugeGraph clearGraph admin API — destructive, use only for "
                                    + "full reloads into a dedicated graph.");

    public static final Option<Boolean> DELETE_VERTEX_WITH_EDGES =
            Options.key("delete_vertex_with_edges")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "When true, DELETE rows for vertices will cascade-delete associated edges. "
                                    + "Default false: only the vertex itself is deleted.");

    // --- Legacy options (deprecated, kept for backward compatibility) ---

    public static final Option<SchemaConfig> SCHEMA_CONFIG =
            Options.key("schema_config")
                    .objectType(SchemaConfig.class)
                    .noDefaultValue()
                    .withDescription(
                            "[Deprecated] Use 'mappings' instead. Legacy schema configuration object "
                                    + "that describes the mapping to a vertex or edge.");

    public static final Option<List<String>> SELECTED_FIELDS =
            Options.key("selected_fields")
                    .listType()
                    .noDefaultValue()
                    .withDescription(
                            "[Deprecated] Use 'properties' within each mapping instead. Selected fields.");

    public static final Option<List<String>> IGNORED_FIELDS =
            Options.key("ignored_fields")
                    .listType()
                    .noDefaultValue()
                    .withDescription(
                            "[Deprecated] Use 'properties' within each mapping instead. Ignored fields.");
}
