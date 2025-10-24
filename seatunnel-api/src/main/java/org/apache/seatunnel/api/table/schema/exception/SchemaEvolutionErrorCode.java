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

package org.apache.seatunnel.api.table.schema.exception;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;

public enum SchemaEvolutionErrorCode implements SeaTunnelErrorCode {

    // Schema Coordination Errors
    SCHEMA_COORDINATOR_NOT_INITIALIZED("SE-01", "Schema coordinator is not initialized"),
    SCHEMA_CHANGE_ALREADY_IN_PROGRESS(
            "SE-02", "Schema change is already in progress for the table"),
    SCHEMA_CHANGE_TIMEOUT("SE-03", "Schema change operation timed out"),
    SCHEMA_CHANGE_COORDINATION_FAILED("SE-04", "Schema change coordination failed"),

    // Schema Validation Errors
    INVALID_SCHEMA_STRUCTURE("SE-05", "Invalid schema structure provided"),
    SCHEMA_INCOMPATIBLE("SE-06", "Schema change is incompatible with current schema"),
    OUTDATED_SCHEMA_EVENT("SE-07", "Schema change event is outdated"),
    UNSUPPORTED_SCHEMA_CHANGE_TYPE("SE-08", "Schema change type is not supported"),

    // Sink Writer Errors
    SCHEMA_CHANGE_APPLICATION_FAILED("SE-09", "Failed to apply schema change to sink writer"),
    FLUSH_OPERATION_FAILED("SE-10", "Flush operation failed during schema evolution"),
    SCHEMA_ROLLBACK_FAILED("SE-11", "Failed to rollback schema change"),

    // Table and Database Errors
    TABLE_SCHEMA_UPDATE_FAILED("SE-12", "Failed to update table schema in database"),
    TABLE_NOT_FOUND("SE-13", "Target table not found"),
    INSUFFICIENT_PERMISSIONS("SE-14", "Insufficient permissions to modify table schema"),

    // Event Processing Errors
    SCHEMA_EVENT_PROCESSING_FAILED("SE-15", "Failed to process schema change event"),
    FLUSH_EVENT_PROCESSING_FAILED("SE-16", "Failed to process flush event"),
    SCHEMA_EVENT_DESERIALIZATION_FAILED("SE-17", "Failed to deserialize schema change event"),

    // Resource Management Errors
    RESOURCE_CLEANUP_FAILED("SE-18", "Failed to cleanup resources after schema change"),
    CONNECTION_POOL_EXHAUSTED("SE-19", "Database connection pool exhausted during schema change"),
    MEMORY_ALLOCATION_FAILED("SE-20", "Memory allocation failed during schema processing");

    private final String code;
    private final String description;

    SchemaEvolutionErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.description;
    }
}
