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

package org.apache.seatunnel.api.sink;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.utils.ExceptionUtils;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Logs dirty records and applies an optional count threshold.
 *
 * <p>The default counter belongs to this collector instance, and payload values remain hidden
 * unless full logging is explicitly configured.
 */
@Slf4j
public class LogDirtyRecordCollector implements DirtyRecordCollector {

    public static final Option<String> LOG_LEVEL =
            Options.key("log_level")
                    .stringType()
                    .defaultValue("ERROR")
                    .withDescription("Log level used for dirty records.");
    public static final Option<Boolean> INCLUDE_STACK_TRACE =
            Options.key("include_stack_trace")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Include the exception message when logging dirty records.");
    public static final Option<Long> THRESHOLD =
            Options.key("threshold")
                    .longType()
                    .defaultValue(-1L)
                    .withDescription(
                            "Dirty record count at which the threshold action runs; non-positive values disable it.");
    public static final Option<Boolean> FAIL_ON_THRESHOLD =
            Options.key("fail_on_threshold")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Fail the job when the configured threshold is reached.");
    public static final Option<String> LOG_PAYLOAD =
            Options.key("log_payload")
                    .stringType()
                    .defaultValue("false")
                    .withDescription(
                            "Payload detail: false logs arity, fields logs field names, full logs values.");
    public static final Option<List<String>> MASK_FIELDS =
            Options.key("mask_fields")
                    .stringType()
                    .asList()
                    .defaultValue(Collections.emptyList())
                    .withDescription(
                            "Case-insensitive field names masked when log_payload is full.");

    private static final long serialVersionUID = 1L;

    private String logLevel = "ERROR";
    private boolean includeStackTrace = true;
    private long threshold = -1;
    private boolean failOnThreshold = false;
    private final AtomicBoolean thresholdNoticeLogged = new AtomicBoolean();
    private String logPayload = "false";
    private Set<String> maskFields = new HashSet<>();
    private DistributedCounter dirtyRecordCounter = new LocalAtomicCounter();

    @Override
    public void setDistributedCounter(DistributedCounter counter) {
        this.dirtyRecordCounter = counter != null ? counter : new LocalAtomicCounter();
        log.debug(
                "[LogCollector] distributed counter set: {}",
                dirtyRecordCounter.getClass().getName());
    }

    @Override
    public void incrementDistributedCounter() {
        dirtyRecordCounter.add(1L);
    }

    @Override
    public void init(Config config) {
        if (config.hasPath(LOG_LEVEL.key())) {
            this.logLevel = config.getString(LOG_LEVEL.key()).toUpperCase(Locale.ROOT);
        }
        if (config.hasPath(INCLUDE_STACK_TRACE.key())) {
            this.includeStackTrace = config.getBoolean(INCLUDE_STACK_TRACE.key());
        }
        if (config.hasPath(THRESHOLD.key())) {
            this.threshold = config.getLong(THRESHOLD.key());
        }
        if (config.hasPath(FAIL_ON_THRESHOLD.key())) {
            this.failOnThreshold = config.getBoolean(FAIL_ON_THRESHOLD.key());
        }
        if (config.hasPath(LOG_PAYLOAD.key())) {
            this.logPayload = config.getString(LOG_PAYLOAD.key()).toLowerCase(Locale.ROOT);
        }
        if (config.hasPath(MASK_FIELDS.key())) {
            this.maskFields =
                    config.getStringList(MASK_FIELDS.key()).stream()
                            .map(value -> value.toLowerCase(Locale.ROOT))
                            .collect(Collectors.toSet());
        }
        if ("full".equals(logPayload)) {
            log.warn("Dirty collector payload logging is enabled in full mode");
        }
    }

    @Override
    public void init(Config config, CatalogTable catalogTable) throws Exception {
        if (config != null && config.hasPath("dirty.collector")) {
            init(config.getConfig("dirty.collector"));
        } else if (config != null) {
            init(config);
        } else {
            DirtyRecordCollector.super.init(config, catalogTable);
        }
    }

    @Override
    public void collect(
            int subTaskIndex,
            Object dirtyRecord,
            Throwable exception,
            String errorMessage,
            CatalogTable catalogTable) {
        incrementDistributedCounter();
        long currentCount = dirtyRecordCounter.value();

        String logMessage =
                String.format(
                        "Dirty record collected (exception) - SubTask: %d, %s, Count: %d, Record: %s, Error: %s",
                        subTaskIndex,
                        formatTableInfo(catalogTable),
                        currentCount,
                        summarizeRecord(dirtyRecord, catalogTable),
                        errorMessage != null ? errorMessage : "");

        if (includeStackTrace && exception != null) {
            logMessage += "\nException: " + ExceptionUtils.getMessage(exception);
        }

        doLog(logMessage);
        checkThresholdRuntime();
    }

    @Override
    public void collectFromUserRule(
            int subTaskIndex, Object record, String errorMessage, CatalogTable catalogTable) {
        incrementDistributedCounter();
        long currentCount = dirtyRecordCounter.value();

        String logMessage =
                String.format(
                        "Dirty record collected (user rule) - SubTask: %d, %s, Count: %d, Record: %s, Reason: %s",
                        subTaskIndex,
                        formatTableInfo(catalogTable),
                        currentCount,
                        summarizeRecord(record, catalogTable),
                        errorMessage != null ? errorMessage : "");

        doLog(logMessage);
        checkThresholdRuntime();
    }

    @Override
    public long getDirtyRecordCount() {
        return dirtyRecordCounter.value();
    }

    @Override
    public void checkThreshold() throws Exception {
        if (threshold <= 0) {
            return;
        }

        long count = dirtyRecordCounter.value();
        if (count >= threshold) {
            String message =
                    String.format("Dirty record threshold exceeded: %d >= %d", count, threshold);
            if (failOnThreshold) {
                log.error(message + " - Task will be failed!");
                throw new RuntimeException(message);
            } else if (thresholdNoticeLogged.compareAndSet(false, true)) {
                log.warn(message + " - Threshold reached but task continues");
            }
        }
    }

    private static String formatTableInfo(CatalogTable catalogTable) {
        return catalogTable != null
                ? String.format(
                        "Table: %s.%s.%s",
                        catalogTable.getTableId().getDatabaseName(),
                        catalogTable.getTableId().getSchemaName(),
                        catalogTable.getTableId().getTableName())
                : "Table: unknown";
    }

    private String summarizeRecord(Object dirtyRecord, CatalogTable catalogTable) {
        if (!(dirtyRecord instanceof SeaTunnelRow)) {
            return dirtyRecord == null
                    ? "null"
                    : "payloadType=" + dirtyRecord.getClass().getSimpleName();
        }
        SeaTunnelRow row = (SeaTunnelRow) dirtyRecord;
        if ("fields".equals(logPayload)) {
            return summarizeFieldNames(row, catalogTable);
        }
        if ("full".equals(logPayload)) {
            return summarizeFullRow(row, catalogTable);
        }
        return "fields=" + row.getArity();
    }

    private String summarizeFieldNames(SeaTunnelRow row, CatalogTable catalogTable) {
        List<String> fieldNames = getFieldNames(row, catalogTable);
        List<String> nonNullFields = new ArrayList<>();
        for (int i = 0; i < row.getArity(); i++) {
            if (row.getField(i) != null) {
                nonNullFields.add(fieldNames.get(i));
            }
        }
        return "nonNullFields=" + nonNullFields;
    }

    private String summarizeFullRow(SeaTunnelRow row, CatalogTable catalogTable) {
        List<String> fieldNames = getFieldNames(row, catalogTable);
        List<String> values = new ArrayList<>();
        for (int i = 0; i < row.getArity(); i++) {
            String fieldName = fieldNames.get(i);
            Object value = row.getField(i);
            values.add(
                    fieldName
                            + "="
                            + (maskFields.contains(fieldName.toLowerCase(Locale.ROOT))
                                    ? "***"
                                    : value));
        }
        return values.toString();
    }

    private List<String> getFieldNames(SeaTunnelRow row, CatalogTable catalogTable) {
        if (catalogTable == null || catalogTable.getTableSchema() == null) {
            return buildPositionalFieldNames(row.getArity());
        }
        List<Column> columns = catalogTable.getTableSchema().getColumns();
        if (columns == null || columns.size() != row.getArity()) {
            return buildPositionalFieldNames(row.getArity());
        }
        return columns.stream().map(Column::getName).collect(Collectors.toList());
    }

    private List<String> buildPositionalFieldNames(int arity) {
        List<String> fieldNames = new ArrayList<>(arity);
        for (int i = 0; i < arity; i++) {
            fieldNames.add("field_" + i);
        }
        return fieldNames;
    }

    private void doLog(String logMessage) {
        switch (logLevel) {
            case "ERROR":
                log.error(logMessage);
                break;
            case "WARN":
                log.warn(logMessage);
                break;
            case "INFO":
                log.info(logMessage);
                break;
            case "DEBUG":
                log.debug(logMessage);
                break;
            default:
                log.error(logMessage);
        }
    }

    private void checkThresholdRuntime() {
        try {
            checkThreshold();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
