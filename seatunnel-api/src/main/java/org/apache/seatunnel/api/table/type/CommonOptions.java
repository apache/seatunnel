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

package org.apache.seatunnel.api.table.type;

import org.apache.seatunnel.api.table.catalog.Column;

import lombok.Getter;

/**
 * Common option keys of SeaTunnel {@link Column#getOptions()} / {@link SeaTunnelRow#getOptions()}.
 * Used to store some extra information of the column value.
 */
@Getter
public enum CommonOptions {
    /**
     * The key of {@link Column#getOptions()} to specify the column value is a json format string.
     */
    JSON("Json", false),
    /** The key of {@link Column#getOptions()} to specify the column value is a metadata field. */
    METADATA("Metadata", false),
    /**
     * The key of {@link SeaTunnelRow#getOptions()} to store the partition value of the row value.
     */
    PARTITION("Partition", true),
    /**
     * The key of {@link SeaTunnelRow#getOptions()} to store the DATABASE value of the row value.
     */
    DATABASE("Database", true),
    /** The key of {@link SeaTunnelRow#getOptions()} to store the TABLE value of the row value. */
    TABLE("Table", true),
    /**
     * The key of {@link SeaTunnelRow#getOptions()} to store the ROW_KIND value of the row value.
     */
    ROW_KIND("RowKind", true),
    /**
     * The key of {@link SeaTunnelRow#getOptions()} to store the EVENT_TIME value of the row value.
     * And the data should be milliseconds.
     */
    EVENT_TIME("EventTime", true),
    /**
     * The key of {@link SeaTunnelRow#getOptions()} to store the DELAY value of the row value. And
     * the data should be milliseconds.
     */
    DELAY("Delay", true),
    /**
     * The key of {@link SeaTunnelRow#getOptions()} to store the binlog filename of the CDC event.
     * Only available for MySQL-CDC source.
     */
    BINLOG_FILE("BinlogFile", true),
    /**
     * The key of {@link SeaTunnelRow#getOptions()} to store the binlog position of the CDC event.
     * Only available for MySQL-CDC source.
     */
    BINLOG_POS("BinlogPos", true),
    /**
     * The key of {@link SeaTunnelRow#getOptions()} to store the row index within the binlog event.
     * Only available for MySQL-CDC source.
     */
    BINLOG_ROW("BinlogRow", true),
    /**
     * The key of {@link SeaTunnelRow#getOptions()} to store the GTID of the CDC event. Only
     * available for MySQL-CDC source when GTID mode is enabled. Format: {@code
     * server_uuid:transaction_id}, e.g. {@code 3E11FA47-71CA-11E1-9E33-C80AA9429562:23}. Null for
     * snapshot rows and when GTID is disabled.
     */
    GTID("Gtid", true),
    /**
     * The key of {@link SeaTunnelRow#getOptions()} to store the timestamp (ms) at which the change
     * was committed in the source database ({@code source.ts_ms} in the Debezium envelope).
     * Available for all CDC connectors. Distinct from {@link #EVENT_TIME}, which is the time the
     * connector processed the event. Null if the connector does not populate {@code source.ts_ms}.
     */
    SOURCE_TIMESTAMP("SourceTimestamp", true),
    /**
     * The key of {@link SeaTunnelRow#getOptions()} to indicate whether the row represents a
     * complete file.
     */
    IS_COMPLETE("is_complete", true),
    /**
     * The key of {@link SeaTunnelRow#getOptions()} to indicate whether the row contains binary
     * format data.
     */
    IS_BINARY_FORMAT("is_binary_format", true);

    private final String name;
    private final boolean supportMetadataTrans;

    CommonOptions(String name, boolean supportMetadataTrans) {
        this.name = name;
        this.supportMetadataTrans = supportMetadataTrans;
    }

    public static CommonOptions fromName(String name) {
        for (CommonOptions option : CommonOptions.values()) {
            if (option.getName().equals(name)) {
                return option;
            }
        }
        throw new IllegalArgumentException("Unknown option name: " + name);
    }
}
