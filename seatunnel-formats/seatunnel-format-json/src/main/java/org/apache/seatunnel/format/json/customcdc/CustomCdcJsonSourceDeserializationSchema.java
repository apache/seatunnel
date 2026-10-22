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

package org.apache.seatunnel.format.json.customcdc;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.format.json.JsonToRowConverters;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.ReadContext;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/** Deserializes a user-defined CDC JSON envelope into the configured table-specific rows. */
@Slf4j
public class CustomCdcJsonSourceDeserializationSchema
        implements DeserializationSchema<SeaTunnelRow> {

    private static final long serialVersionUID = 1L;
    private static final Option[] JSON_PATH_OPTIONS = {
        Option.SUPPRESS_EXCEPTIONS, Option.DEFAULT_PATH_LEAF_TO_NULL
    };
    private static final Configuration JSON_PATH_CONFIGURATION =
            Configuration.defaultConfiguration().addOptions(JSON_PATH_OPTIONS);

    private final CompiledJsonPath schemaPath;
    private final CompiledJsonPath tablePath;
    private final CompiledJsonPath operationPath;
    private final CompiledJsonPath beforePath;
    private final CompiledJsonPath afterPath;
    private final Map<String, OperationType> operationTypeMapping;
    private final Map<String, TableRuntime> runtimeByFullName = new HashMap<>();
    private final Map<String, TableRuntime> runtimeBySimpleName = new HashMap<>();
    private final Set<String> ambiguousSimpleNames = new HashSet<>();
    private final Map<String, TableRuntime> runtimeByConfiguredSimpleName = new HashMap<>();
    private final Set<String> ambiguousConfiguredSimpleNames = new HashSet<>();

    private CustomCdcJsonSourceDeserializationSchema(
            List<CatalogTable> catalogTables,
            Map<TablePath, Map<String, String>> jsonFieldMappings,
            Set<TablePath> simpleTablePaths,
            String schemaPath,
            String tablePath,
            String operationPath,
            Map<String, String> operationTypeMapping,
            String beforePath,
            String afterPath) {
        this.schemaPath = CompiledJsonPath.optional(schemaPath);
        this.tablePath = CompiledJsonPath.required(tablePath, "table_path");
        this.operationPath = CompiledJsonPath.required(operationPath, "operation_path");
        this.beforePath = CompiledJsonPath.required(beforePath, "before_path");
        this.afterPath = CompiledJsonPath.required(afterPath, "after_path");
        this.operationTypeMapping = createOperationTypeMapping(operationTypeMapping);

        for (CatalogTable catalogTable : catalogTables) {
            TableRuntime runtime =
                    new TableRuntime(
                            catalogTable,
                            jsonFieldMappings.getOrDefault(
                                    catalogTable.getTablePath(), Collections.emptyMap()));
            runtimeByFullName.put(normalize(runtime.getTableId()), runtime);
            registerSimpleRuntime(runtimeBySimpleName, ambiguousSimpleNames, runtime);
            if (simpleTablePaths.contains(catalogTable.getTablePath())) {
                registerSimpleRuntime(
                        runtimeByConfiguredSimpleName, ambiguousConfiguredSimpleNames, runtime);
            }
        }
        ambiguousSimpleNames.forEach(runtimeBySimpleName::remove);
        ambiguousConfiguredSimpleNames.forEach(runtimeByConfiguredSimpleName::remove);
    }

    private static void registerSimpleRuntime(
            Map<String, TableRuntime> runtimes, Set<String> ambiguousNames, TableRuntime runtime) {
        String simpleName = normalize(runtime.getSimpleTableName());
        TableRuntime existingRuntime = runtimes.get(simpleName);
        if (existingRuntime != null
                && !Objects.equals(existingRuntime.getTableId(), runtime.getTableId())) {
            ambiguousNames.add(simpleName);
            return;
        }
        if (!ambiguousNames.contains(simpleName)) {
            runtimes.put(simpleName, runtime);
        }
    }

    @Override
    public SeaTunnelRow deserialize(byte[] message) throws IOException {
        throw new UnsupportedOperationException(
                "Please invoke DeserializationSchema#deserialize(byte[], Collector<SeaTunnelRow>) instead.");
    }

    @Override
    public void deserialize(byte[] message, Collector<SeaTunnelRow> out) {
        if (message == null || message.length == 0) {
            return;
        }
        ReadContext readContext =
                JsonPath.using(JSON_PATH_CONFIGURATION)
                        .parse(new String(message, StandardCharsets.UTF_8));
        String operation = readString(readContext, operationPath);
        String tableName = readString(readContext, tablePath);
        String schemaName = readString(readContext, schemaPath);
        TableRuntime tableRuntime = resolveTableRuntime(schemaName, tableName);
        if (tableRuntime == null) {
            log.debug(
                    "Skip custom CDC record because table is not configured, schema: {}, table: {}",
                    schemaName,
                    tableName);
            return;
        }

        switch (parseOperationType(operation)) {
            case INSERT:
                emitRequiredRow(
                        readValue(readContext, afterPath),
                        tableRuntime,
                        RowKind.INSERT,
                        "after",
                        out);
                return;
            case UPDATE:
                emitRequiredRow(
                        readValue(readContext, beforePath),
                        tableRuntime,
                        RowKind.UPDATE_BEFORE,
                        "before",
                        out);
                emitRequiredRow(
                        readValue(readContext, afterPath),
                        tableRuntime,
                        RowKind.UPDATE_AFTER,
                        "after",
                        out);
                return;
            case UPDATE_BEFORE:
                emitRequiredRow(
                        readValue(readContext, beforePath),
                        tableRuntime,
                        RowKind.UPDATE_BEFORE,
                        "before",
                        out);
                return;
            case UPDATE_AFTER:
                emitRequiredRow(
                        readValue(readContext, afterPath),
                        tableRuntime,
                        RowKind.UPDATE_AFTER,
                        "after",
                        out);
                return;
            case DELETE:
                emitRequiredRow(
                        readValue(readContext, beforePath),
                        tableRuntime,
                        RowKind.DELETE,
                        "before",
                        out);
                return;
            default:
                throw new IllegalStateException("Unsupported CDC operation: " + operation);
        }
    }

    private static void emitRequiredRow(
            Object data,
            TableRuntime tableRuntime,
            RowKind rowKind,
            String side,
            Collector<SeaTunnelRow> out) {
        if (data == null) {
            throw new IllegalStateException(
                    String.format(
                            "CUSTOM_CDC_JSON %s data is null for table %s and row kind %s",
                            side, tableRuntime.getTableId(), rowKind));
        }
        emitRow(data, tableRuntime, rowKind, out);
    }

    private static void emitRow(
            Object data, TableRuntime tableRuntime, RowKind rowKind, Collector<SeaTunnelRow> out) {
        ReadContext dataContext = JsonPath.using(JSON_PATH_CONFIGURATION).parse(data);
        SeaTunnelRow row = new SeaTunnelRow(tableRuntime.getFieldNames().length);
        for (int i = 0; i < tableRuntime.getFieldNames().length; i++) {
            Object fieldValue = readValue(dataContext, tableRuntime.getJsonFieldPaths()[i]);
            JsonNode fieldJsonNode = JsonUtils.toJsonNode(fieldValue);
            row.setField(
                    i,
                    tableRuntime.getConverters()[i].convert(
                            fieldJsonNode, tableRuntime.getFieldNames()[i]));
        }
        row.setTableId(tableRuntime.getTableId());
        row.setRowKind(rowKind);
        out.collect(row);
    }

    private OperationType parseOperationType(String operation) {
        if (operation == null) {
            throw new IllegalStateException("CUSTOM_CDC_JSON operation is null");
        }
        String normalizedOperation = normalize(operation);
        if (operationTypeMapping.isEmpty()) {
            return OperationType.parseDefault(normalizedOperation);
        }
        OperationType configuredType = operationTypeMapping.get(normalizedOperation);
        if (configuredType != null) {
            return configuredType;
        }
        throw new IllegalStateException("Unsupported CUSTOM_CDC_JSON operation: " + operation);
    }

    private TableRuntime resolveTableRuntime(String schemaName, String tableName) {
        if (tableName == null) {
            return null;
        }
        if (schemaName != null) {
            TableRuntime fullNameRuntime =
                    runtimeByFullName.get(normalize(schemaName + "." + tableName));
            return fullNameRuntime != null
                    ? fullNameRuntime
                    : resolveConfiguredSimpleTableRuntime(tableName);
        }
        return resolveSimpleTableRuntime(tableName);
    }

    private TableRuntime resolveConfiguredSimpleTableRuntime(String tableName) {
        String normalizedTableName = normalize(tableName);
        if (ambiguousConfiguredSimpleNames.contains(normalizedTableName)) {
            throw new IllegalStateException(
                    String.format(
                            "Multiple simple schema_list tables share the same table name '%s'",
                            tableName));
        }
        return runtimeByConfiguredSimpleName.get(normalizedTableName);
    }

    private TableRuntime resolveSimpleTableRuntime(String tableName) {
        String normalizedTableName = normalize(tableName);
        if (ambiguousSimpleNames.contains(normalizedTableName)) {
            throw new IllegalStateException(
                    String.format(
                            "Multiple schema_list tables share the same table name '%s', please use "
                                    + "schema-qualified table config together with schema_path",
                            tableName));
        }
        return runtimeBySimpleName.get(normalizedTableName);
    }

    private static String readString(ReadContext context, CompiledJsonPath path) {
        Object value = readValue(context, path);
        return value == null ? null : Objects.toString(value, null);
    }

    private static Object readValue(ReadContext context, CompiledJsonPath path) {
        return path == null ? null : context.read(path.get());
    }

    private static String normalize(String value) {
        return value == null ? null : value.trim().toLowerCase(Locale.ROOT);
    }

    private static Map<String, OperationType> createOperationTypeMapping(
            Map<String, String> configuredMapping) {
        if (configuredMapping == null || configuredMapping.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, OperationType> normalizedMapping = new HashMap<>();
        configuredMapping.forEach(
                (operationValue, logicalType) -> {
                    String normalizedOperationValue = normalize(operationValue);
                    if (normalizedOperationValue == null || normalizedOperationValue.isEmpty()) {
                        throw new IllegalArgumentException(
                                "CUSTOM_CDC_JSON operation mapping contains blank operation value");
                    }
                    normalizedMapping.put(
                            normalizedOperationValue,
                            OperationType.parseConfiguredType(logicalType));
                });
        return normalizedMapping;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        throw new UnsupportedOperationException("CUSTOM_CDC_JSON has a table-specific row type.");
    }

    public static Builder builder(List<CatalogTable> catalogTables) {
        return new Builder(catalogTables);
    }

    private enum OperationType {
        INSERT,
        UPDATE,
        UPDATE_BEFORE,
        UPDATE_AFTER,
        DELETE;

        private static OperationType parseDefault(String normalizedOperation) {
            switch (normalizedOperation) {
                case "c":
                case "r":
                case "create":
                case "insert":
                    return INSERT;
                case "u":
                case "update":
                    return UPDATE;
                case "update_before":
                    return UPDATE_BEFORE;
                case "update_after":
                    return UPDATE_AFTER;
                case "d":
                case "delete":
                    return DELETE;
                default:
                    throw new IllegalStateException(
                            "Unsupported CUSTOM_CDC_JSON operation: " + normalizedOperation);
            }
        }

        private static OperationType parseConfiguredType(String configuredType) {
            String normalizedType = normalize(configuredType);
            if (normalizedType == null || normalizedType.isEmpty()) {
                throw new IllegalArgumentException(
                        "CUSTOM_CDC_JSON operation mapping target is blank");
            }
            switch (normalizedType) {
                case "insert":
                    return INSERT;
                case "update":
                    return UPDATE;
                case "update_before":
                    return UPDATE_BEFORE;
                case "update_after":
                    return UPDATE_AFTER;
                case "delete":
                    return DELETE;
                default:
                    throw new IllegalArgumentException(
                            "Unsupported CUSTOM_CDC_JSON operation mapping target: "
                                    + configuredType);
            }
        }
    }

    public static class Builder {
        private final List<CatalogTable> catalogTables;
        private Map<TablePath, Map<String, String>> jsonFieldMappings = Collections.emptyMap();
        private String schemaPath;
        private String tablePath;
        private String operationPath;
        private Map<String, String> operationTypeMapping = Collections.emptyMap();
        private Set<TablePath> simpleTablePaths;
        private String beforePath;
        private String afterPath;

        private Builder(List<CatalogTable> catalogTables) {
            if (catalogTables == null || catalogTables.isEmpty()) {
                throw new IllegalArgumentException(
                        "CUSTOM_CDC_JSON requires at least one configured catalog table.");
            }
            this.catalogTables = catalogTables;
        }

        public Builder setJsonFieldMappings(Map<TablePath, Map<String, String>> jsonFieldMappings) {
            this.jsonFieldMappings = jsonFieldMappings;
            return this;
        }

        public Builder setSchemaPath(String schemaPath) {
            this.schemaPath = schemaPath;
            return this;
        }

        public Builder setTablePath(String tablePath) {
            this.tablePath = tablePath;
            return this;
        }

        public Builder setOperationPath(String operationPath) {
            this.operationPath = operationPath;
            return this;
        }

        public Builder setOperationTypeMapping(Map<String, String> operationTypeMapping) {
            this.operationTypeMapping = operationTypeMapping;
            return this;
        }

        public Builder setSimpleTablePaths(Set<TablePath> simpleTablePaths) {
            this.simpleTablePaths = simpleTablePaths;
            return this;
        }

        public Builder setBeforePath(String beforePath) {
            this.beforePath = beforePath;
            return this;
        }

        public Builder setAfterPath(String afterPath) {
            this.afterPath = afterPath;
            return this;
        }

        public CustomCdcJsonSourceDeserializationSchema build() {
            Set<TablePath> effectiveSimpleTablePaths =
                    simpleTablePaths == null
                            ? catalogTables.stream()
                                    .map(CatalogTable::getTablePath)
                                    .collect(Collectors.toSet())
                            : simpleTablePaths;
            return new CustomCdcJsonSourceDeserializationSchema(
                    catalogTables,
                    jsonFieldMappings,
                    effectiveSimpleTablePaths,
                    schemaPath,
                    tablePath,
                    operationPath,
                    operationTypeMapping,
                    beforePath,
                    afterPath);
        }
    }

    @Getter
    private static class TableRuntime implements Serializable {
        private static final long serialVersionUID = 1L;
        private final String tableId;
        private final String simpleTableName;
        private final String[] fieldNames;
        private final CompiledJsonPath[] jsonFieldPaths;
        private final JsonToRowConverters.JsonToObjectConverter[] converters;

        private TableRuntime(CatalogTable catalogTable, Map<String, String> jsonFieldMapping) {
            SeaTunnelRowType rowType = catalogTable.getSeaTunnelRowType();
            this.tableId = catalogTable.getTableId().toTablePath().toString();
            this.simpleTableName = catalogTable.getTablePath().getTableName();
            this.fieldNames = rowType.getFieldNames();
            this.jsonFieldPaths = new CompiledJsonPath[fieldNames.length];
            this.converters = new JsonToRowConverters.JsonToObjectConverter[fieldNames.length];
            JsonToRowConverters jsonToRowConverters = new JsonToRowConverters(false, false);
            for (int i = 0; i < fieldNames.length; i++) {
                String fieldName = fieldNames[i];
                jsonFieldPaths[i] =
                        CompiledJsonPath.required(
                                jsonFieldMapping.getOrDefault(fieldName, "$." + fieldName),
                                "json_field." + fieldName);
                converters[i] = jsonToRowConverters.createConverter(rowType.getFieldType(i));
            }
        }
    }

    private static class CompiledJsonPath implements Serializable {
        private static final long serialVersionUID = 1L;
        private final String expression;
        private transient JsonPath compiled;

        private CompiledJsonPath(String expression) {
            this.expression = expression;
        }

        private static CompiledJsonPath optional(String expression) {
            return expression == null ? null : required(expression, "JSONPath");
        }

        private static CompiledJsonPath required(String expression, String optionName) {
            if (expression == null || expression.trim().isEmpty()) {
                throw new IllegalArgumentException("CUSTOM_CDC_JSON requires " + optionName);
            }
            CompiledJsonPath path = new CompiledJsonPath(expression);
            path.get();
            return path;
        }

        private JsonPath get() {
            if (compiled == null) {
                compiled = JsonPath.compile(expression);
            }
            return compiled;
        }
    }
}
