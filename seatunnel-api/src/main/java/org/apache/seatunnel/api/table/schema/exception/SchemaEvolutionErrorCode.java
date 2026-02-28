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

    // schema coordination errors
    SCHEMA_COORDINATOR_NOT_INITIALIZED("SE-01", "Schema coordinator is not initialized"),
    SCHEMA_CHANGE_ALREADY_IN_PROGRESS(
            "SE-02", "Schema change is already in progress for the table"),
    SCHEMA_CHANGE_TIMEOUT("SE-03", "Schema change operation timed out"),
    SCHEMA_CHANGE_COORDINATION_FAILED("SE-04", "Schema change coordination failed"),

    // schema validation errors
    INVALID_SCHEMA_STRUCTURE("SE-05", "Invalid schema structure provided"),
    OUTDATED_SCHEMA_EVENT("SE-06", "Schema change event is outdated"),
    UNSUPPORTED_SCHEMA_CHANGE_TYPE("SE-07", "Schema change type is not supported"),

    // sink writer errors
    SCHEMA_CHANGE_APPLICATION_FAILED("SE-08", "Failed to apply schema change to sink writer"),
    FLUSH_OPERATION_FAILED("SE-09", "Flush operation failed during schema evolution"),

    // event processing errors
    SCHEMA_EVENT_PROCESSING_FAILED("SE-10", "Failed to process schema change event"),

    // meta lake schema
    GET_META_LAKE_TABLE_SCHEMA_FAILED("SE-11", "Get meta lake table schema failed"),
    ERROR_INVALID_TABLE_URL(
            "SE-12",
            "Invalid table URL format, expected: /catalogs/{catalog}/schemas/{schema}/tables/{table}"),
    CATALOG_TABLE_SIZE_IS_ERROR("SE-13", "Catalog table size is error");

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
