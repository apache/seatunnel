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

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.utils.ExceptionUtils;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/** A dirty record collector that logs dirty data information. */
@Slf4j
public class LogDirtyRecordCollector implements DirtyRecordCollector {

    private static final long serialVersionUID = 1L;

    private String logLevel = "ERROR";
    private boolean includeStackTrace = true;
    private long threshold = -1;
    private boolean failOnThreshold = false;
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
        if (config.hasPath("log_level")) {
            this.logLevel = config.getString("log_level").toUpperCase();
        }
        if (config.hasPath("include_stack_trace")) {
            this.includeStackTrace = config.getBoolean("include_stack_trace");
        }
        if (config.hasPath("threshold")) {
            this.threshold = config.getLong("threshold");
        }
        if (config.hasPath("fail_on_threshold")) {
            this.failOnThreshold = config.getBoolean("fail_on_threshold");
        }
        if (config.hasPath("log_payload")) {
            this.logPayload = config.getString("log_payload").toLowerCase();
        }
        if (config.hasPath("mask_fields")) {
            this.maskFields =
                    config.getStringList("mask_fields").stream()
                            .map(String::toLowerCase)
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
                            + (maskFields.contains(fieldName.toLowerCase()) ? "***" : value));
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
            } else {
                log.warn(message + " - Threshold reached but task continues");
            }
        }
    }
}
