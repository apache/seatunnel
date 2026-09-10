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

package org.apache.seatunnel.translation.flink.schema;

import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.util.HashMap;
import java.util.Map;

/** Internal data-plane control messages used by Flink schema evolution operators. */
public final class SchemaEvolutionControlMessage {

    private static final char SEQUENCE_SEPARATOR = '#';

    public static final String SCHEMA_CHANGE_BROADCAST = "schema_change_broadcast";
    public static final String SCHEMA_CHANGE_EVENT = "schema_change_event";
    public static final String SCHEMA_CHANGE_ID = "schema_change_id";
    public static final String SCHEMA_CHANGE_FILTERED = "schema_change_filtered";
    public static final String REQUIRED_SCHEMA_CHANGE_ID = "required_schema_change_id";

    private SchemaEvolutionControlMessage() {}

    public static boolean isSchemaBroadcast(SeaTunnelRow row) {
        return row != null
                && row.getOptions() != null
                && row.getOptions().get(SCHEMA_CHANGE_BROADCAST) instanceof SchemaChangeEvent;
    }

    public static SchemaChangeEvent schemaChangeEvent(SeaTunnelRow row) {
        if (!isSchemaBroadcast(row)) {
            return null;
        }
        return (SchemaChangeEvent) row.getOptions().get(SCHEMA_CHANGE_BROADCAST);
    }

    public static String schemaChangeId(String producerId, long sequence) {
        return producerId + SEQUENCE_SEPARATOR + sequence;
    }

    public static String schemaChangeProducerId(String schemaChangeId) {
        int separator =
                schemaChangeId == null ? -1 : schemaChangeId.lastIndexOf(SEQUENCE_SEPARATOR);
        if (separator <= 0 || separator == schemaChangeId.length() - 1) {
            return null;
        }
        return schemaChangeId.substring(0, separator);
    }

    public static long schemaChangeSequence(String schemaChangeId) {
        int separator =
                schemaChangeId == null ? -1 : schemaChangeId.lastIndexOf(SEQUENCE_SEPARATOR);
        if (separator <= 0 || separator == schemaChangeId.length() - 1) {
            return -1L;
        }
        try {
            return Long.parseLong(schemaChangeId.substring(separator + 1));
        } catch (NumberFormatException ignored) {
            return -1L;
        }
    }

    public static String schemaChangeId(SeaTunnelRow row) {
        if (row != null && row.getOptions() != null) {
            Object eventId = row.getOptions().get(SCHEMA_CHANGE_ID);
            if (eventId instanceof String) {
                return (String) eventId;
            }
        }
        return null;
    }

    public static boolean isFilteredSchemaChange(SeaTunnelRow row) {
        return row != null
                && row.getOptions() != null
                && Boolean.TRUE.equals(row.getOptions().get(SCHEMA_CHANGE_FILTERED));
    }

    /** Maps a schema command through a transform while retaining its source-side dependency ID. */
    public static SeaTunnelRow transformedSchemaChangeRow(
            SeaTunnelRow sourceRow, SchemaChangeEvent transformedEvent) {
        Map<String, Object> options = new HashMap<>(sourceRow.getOptions());
        if (transformedEvent == null) {
            options.put(SCHEMA_CHANGE_FILTERED, true);
        } else {
            options.put(SCHEMA_CHANGE_BROADCAST, transformedEvent);
            options.remove(SCHEMA_CHANGE_FILTERED);
        }
        SeaTunnelRow transformedRow = new SeaTunnelRow(0);
        transformedRow.setOptions(options);
        return transformedRow;
    }

    public static void requireSchemaChange(SeaTunnelRow row, String schemaChangeId) {
        if (row == null || schemaChangeId == null) {
            return;
        }
        Map<String, Object> options =
                row.getOptions() == null ? new HashMap<>() : new HashMap<>(row.getOptions());
        options.put(REQUIRED_SCHEMA_CHANGE_ID, schemaChangeId);
        row.setOptions(options);
    }

    /** Copies an internal schema dependency when a transform creates a replacement row. */
    public static void copyRequiredSchemaChange(SeaTunnelRow source, SeaTunnelRow target) {
        if (target == null) {
            return;
        }
        String requiredChangeId = requiredSchemaChangeId(source);
        if (requiredChangeId == null) {
            return;
        }
        Map<String, Object> options =
                target.getOptions() == null ? new HashMap<>() : new HashMap<>(target.getOptions());
        options.put(REQUIRED_SCHEMA_CHANGE_ID, requiredChangeId);
        target.setOptions(options);
    }

    public static String requiredSchemaChangeId(SeaTunnelRow row) {
        if (row == null || row.getOptions() == null) {
            return null;
        }
        Object requiredChangeId = row.getOptions().get(REQUIRED_SCHEMA_CHANGE_ID);
        return requiredChangeId instanceof String ? (String) requiredChangeId : null;
    }

    public static void clearRequiredSchemaChange(SeaTunnelRow row) {
        if (row == null
                || row.getOptions() == null
                || !row.getOptions().containsKey(REQUIRED_SCHEMA_CHANGE_ID)) {
            return;
        }
        Map<String, Object> options = new HashMap<>(row.getOptions());
        options.remove(REQUIRED_SCHEMA_CHANGE_ID);
        row.setOptions(options.isEmpty() ? null : options);
    }

    public static SeaTunnelRow sinkSchemaChangeRow(SchemaChangeEvent event, String schemaChangeId) {
        SeaTunnelRow schemaRow = new SeaTunnelRow(0);
        Map<String, Object> options = new HashMap<>();
        options.put(SCHEMA_CHANGE_EVENT, event);
        options.put(SCHEMA_CHANGE_ID, schemaChangeId);
        schemaRow.setOptions(options);
        return schemaRow;
    }
}
