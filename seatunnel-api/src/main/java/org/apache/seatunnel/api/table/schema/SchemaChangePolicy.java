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

package org.apache.seatunnel.api.table.schema;

import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.schema.exception.SchemaEvolutionErrorCode;
import org.apache.seatunnel.api.table.schema.exception.SchemaEvolutionException;

import java.util.List;

public final class SchemaChangePolicy {

    private SchemaChangePolicy() {}

    public static SchemaChangeBehavior resolveBehavior(Object source) {
        if (source instanceof org.apache.seatunnel.api.source.SupportSchemaChangeBehavior) {
            return ((org.apache.seatunnel.api.source.SupportSchemaChangeBehavior) source)
                    .getSchemaChangeBehavior();
        }
        return SchemaChangeBehavior.EVOLVE;
    }

    public static void validateStrict(
            SchemaChangeBehavior behavior, SchemaChangeEvent event, String jobId) {
        if (behavior == SchemaChangeBehavior.STRICT) {
            throw policyException(
                    event,
                    jobId,
                    String.format(
                            "Schema change behavior is STRICT. Failing because schema change event %s was observed for table %s.",
                            event.getEventType(), event.tableIdentifier()));
        }
    }

    public static void validateIgnore(
            SchemaChangeBehavior behavior, SchemaChangeEvent event, String jobId) {
        if (behavior == SchemaChangeBehavior.IGNORE && !isSafeToIgnore(event)) {
            throw policyException(
                    event,
                    jobId,
                    String.format(
                            "Schema change behavior is IGNORE, but schema change event %s for table %s changes the runtime row layout and cannot be safely ignored.",
                            event.getEventType(), event.tableIdentifier()));
        }
    }

    public static boolean shouldIgnore(SchemaChangeBehavior behavior) {
        return behavior == SchemaChangeBehavior.IGNORE;
    }

    public static boolean isSupported(
            SchemaChangeEvent event, List<SchemaChangeType> supportedTypes) {
        if (supportedTypes == null || supportedTypes.isEmpty()) {
            return false;
        }
        switch (event.getEventType()) {
            case SCHEMA_CHANGE_ADD_COLUMN:
                return supportedTypes.contains(SchemaChangeType.ADD_COLUMN);
            case SCHEMA_CHANGE_DROP_COLUMN:
                return supportedTypes.contains(SchemaChangeType.DROP_COLUMN);
            case SCHEMA_CHANGE_MODIFY_COLUMN:
                return supportedTypes.contains(SchemaChangeType.UPDATE_COLUMN);
            case SCHEMA_CHANGE_CHANGE_COLUMN:
                return supportedTypes.contains(SchemaChangeType.RENAME_COLUMN);
            case SCHEMA_CHANGE_ALTER_TABLE_COMMENT:
                return supportedTypes.contains(SchemaChangeType.ALTER_TABLE_COMMENT);
            case SCHEMA_CHANGE_ALTER_COLUMN_COMMENT:
                return supportedTypes.contains(SchemaChangeType.ALTER_COLUMN_COMMENT);
            case SCHEMA_CHANGE_UPDATE_COLUMNS:
                return supportedTypes.contains(SchemaChangeType.ADD_COLUMN)
                        || supportedTypes.contains(SchemaChangeType.DROP_COLUMN)
                        || supportedTypes.contains(SchemaChangeType.UPDATE_COLUMN)
                        || supportedTypes.contains(SchemaChangeType.RENAME_COLUMN)
                        || supportedTypes.contains(SchemaChangeType.ALTER_COLUMN_COMMENT);
            default:
                return false;
        }
    }

    public static void validateSupported(
            SchemaChangeEvent event, List<SchemaChangeType> supportedTypes, String jobId) {
        if (!isSupported(event, supportedTypes)) {
            throw policyException(
                    event,
                    jobId,
                    String.format(
                            "Schema change event %s for table %s is not supported end to end.",
                            event.getEventType(), event.tableIdentifier()));
        }
    }

    private static boolean isSafeToIgnore(SchemaChangeEvent event) {
        switch (event.getEventType()) {
            case SCHEMA_CHANGE_ALTER_TABLE_COMMENT:
            case SCHEMA_CHANGE_ALTER_COLUMN_COMMENT:
                return true;
            default:
                return false;
        }
    }

    private static SchemaEvolutionException policyException(
            SchemaChangeEvent event, String jobId, String message) {
        TableIdentifier tableIdentifier = event == null ? null : event.tableIdentifier();
        return new SchemaEvolutionException(
                SchemaEvolutionErrorCode.SCHEMA_EVENT_PROCESSING_FAILED,
                message,
                tableIdentifier,
                jobId);
    }
}
