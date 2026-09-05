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

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.source.SupportSchemaChangeBehavior;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnsEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.schema.exception.SchemaChangePolicyException;
import org.apache.seatunnel.api.table.schema.exception.SchemaEvolutionErrorCode;
import org.apache.seatunnel.api.table.schema.exception.SchemaEvolutionException;

import java.util.List;

/** Utility methods for enforcing source schema change behavior before downstream coordination. */
public final class SchemaChangePolicy {

    private SchemaChangePolicy() {}

    /**
     * Resolves the schema change behavior advertised by a source.
     *
     * <p>Sources that do not implement {@link SupportSchemaChangeBehavior} default to {@link
     * SchemaChangeBehavior#EVOLVE}.
     *
     * @param source source instance to inspect
     * @return configured schema change behavior, or {@link SchemaChangeBehavior#EVOLVE}
     */
    public static SchemaChangeBehavior resolveBehavior(Object source) {
        if (source instanceof SupportSchemaChangeBehavior) {
            return ((SupportSchemaChangeBehavior) source).getSchemaChangeBehavior();
        }
        return SchemaChangeBehavior.EVOLVE;
    }

    /**
     * Fails fast when strict mode observes any schema change event.
     *
     * @param behavior resolved source behavior
     * @param event schema change event emitted by the source
     * @param jobId job identifier used in the raised exception
     */
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

    /**
     * Validates whether ignore mode can safely drop the event before downstream propagation.
     *
     * @param behavior resolved source behavior
     * @param event schema change event emitted by the source
     * @param jobId job identifier used in the raised exception
     */
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

    /**
     * Returns whether schema change events should be dropped before downstream coordination.
     *
     * @param behavior resolved source behavior
     * @return true when the behavior is {@link SchemaChangeBehavior#IGNORE}
     */
    public static boolean shouldIgnore(SchemaChangeBehavior behavior) {
        return behavior == SchemaChangeBehavior.IGNORE;
    }

    /**
     * Returns whether a sink writer explicitly overrides the deprecated schema-change callback.
     *
     * <p>This compatibility check lets existing writers migrate to {@code
     * SupportSchemaEvolutionSinkWriter} without being mistaken for the inherited no-op method.
     *
     * @param sinkWriter sink writer to inspect
     * @return true when the writer declares its own {@code applySchemaChange} implementation
     */
    public static boolean overridesDeprecatedSchemaChangeMethod(Object sinkWriter) {
        try {
            return sinkWriter
                            .getClass()
                            .getMethod("applySchemaChange", SchemaChangeEvent.class)
                            .getDeclaringClass()
                    != SinkWriter.class;
        } catch (NoSuchMethodException e) {
            return false;
        }
    }

    /**
     * Returns whether the event type is covered by the sink-advertised schema change capabilities.
     *
     * @param event schema change event to validate
     * @param supportedTypes sink-advertised schema change capabilities
     * @return true when the event can be handled end to end
     */
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
                AlterTableColumnsEvent columnsEvent = (AlterTableColumnsEvent) event;
                if (columnsEvent.getEvents().isEmpty()) {
                    return true;
                }
                return columnsEvent.getEvents().stream()
                        .allMatch(subEvent -> isSubEventSupported(subEvent, supportedTypes));
            default:
                return false;
        }
    }

    /**
     * Fails when the event cannot be handled by the sink-advertised schema change capabilities.
     *
     * @param event schema change event to validate
     * @param supportedTypes sink-advertised schema change capabilities
     * @param jobId job identifier used in the raised exception
     */
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

    /**
     * Fails when a sink does not advertise schema evolution support.
     *
     * <p>This rejection is a deterministic policy failure, so it is raised as a {@link
     * SchemaChangePolicyException} and must not trigger checkpoint-based retries.
     *
     * @param supportsSchemaEvolution whether the sink advertises schema evolution support
     * @param event schema change event to validate
     * @param sinkName sink plugin name used in the error message
     * @param jobId job identifier used in the raised exception
     */
    public static void validateSinkSupportsSchemaEvolution(
            boolean supportsSchemaEvolution,
            SchemaChangeEvent event,
            String sinkName,
            String jobId) {
        if (!supportsSchemaEvolution) {
            throw policyException(
                    event,
                    jobId,
                    String.format(
                            "Sink %s does not advertise schema evolution support for event %s.",
                            sinkName, event.getEventType()));
        }
    }

    /**
     * Returns whether an event only changes metadata and can be dropped without changing the
     * runtime row encoding.
     *
     * @param event schema change event to inspect
     * @return true when dropping the event cannot change row decoding
     */
    public static boolean isSafeToIgnore(SchemaChangeEvent event) {
        switch (event.getEventType()) {
            case SCHEMA_CHANGE_ALTER_TABLE_COMMENT:
            case SCHEMA_CHANGE_ALTER_COLUMN_COMMENT:
                return true;
            default:
                return false;
        }
    }

    /**
     * Returns whether a single sub-event inside an {@link AlterTableColumnsEvent} is supported by
     * the given list of sink-advertised capabilities.
     */
    private static boolean isSubEventSupported(
            AlterTableColumnEvent subEvent, List<SchemaChangeType> supportedTypes) {
        switch (subEvent.getEventType()) {
            case SCHEMA_CHANGE_ADD_COLUMN:
                return supportedTypes.contains(SchemaChangeType.ADD_COLUMN);
            case SCHEMA_CHANGE_DROP_COLUMN:
                return supportedTypes.contains(SchemaChangeType.DROP_COLUMN);
            case SCHEMA_CHANGE_MODIFY_COLUMN:
                return supportedTypes.contains(SchemaChangeType.UPDATE_COLUMN);
            case SCHEMA_CHANGE_CHANGE_COLUMN:
                return supportedTypes.contains(SchemaChangeType.RENAME_COLUMN);
            case SCHEMA_CHANGE_ALTER_COLUMN_COMMENT:
                return supportedTypes.contains(SchemaChangeType.ALTER_COLUMN_COMMENT);
            default:
                return false;
        }
    }

    private static SchemaEvolutionException policyException(
            SchemaChangeEvent event, String jobId, String message) {
        TableIdentifier tableIdentifier = event == null ? null : event.tableIdentifier();
        return new SchemaChangePolicyException(
                SchemaEvolutionErrorCode.SCHEMA_EVENT_PROCESSING_FAILED,
                message,
                tableIdentifier,
                jobId);
    }
}
