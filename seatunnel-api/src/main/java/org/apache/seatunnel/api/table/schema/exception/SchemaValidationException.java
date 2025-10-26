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

import org.apache.seatunnel.api.table.catalog.TableIdentifier;

/**
 * Exception thrown when schema validation fails. This includes invalid schema structures,
 * incompatible changes, and outdated events.
 */
public class SchemaValidationException extends SchemaEvolutionException {

    public SchemaValidationException(SchemaEvolutionErrorCode errorCode, String errorMessage) {
        super(errorCode, errorMessage);
    }

    public SchemaValidationException(
            SchemaEvolutionErrorCode errorCode, String errorMessage, Throwable cause) {
        super(errorCode, errorMessage, cause);
    }

    public SchemaValidationException(
            SchemaEvolutionErrorCode errorCode,
            String errorMessage,
            TableIdentifier tableIdentifier,
            String jobId) {
        super(errorCode, errorMessage, tableIdentifier, jobId);
    }

    public SchemaValidationException(
            SchemaEvolutionErrorCode errorCode,
            String errorMessage,
            TableIdentifier tableIdentifier,
            String jobId,
            Throwable cause) {
        super(errorCode, errorMessage, tableIdentifier, jobId, cause);
    }

    /** Create an exception for invalid schema structure */
    public static SchemaValidationException invalidSchema(
            TableIdentifier tableIdentifier, String jobId, String reason) {
        String message = String.format("Invalid schema structure: %s", reason);
        return new SchemaValidationException(
                SchemaEvolutionErrorCode.INVALID_SCHEMA_STRUCTURE, message, tableIdentifier, jobId);
    }

    /** Create an exception for unsupported schema change types */
    public static SchemaValidationException unsupportedChangeType(
            TableIdentifier tableIdentifier, String jobId) {
        return new SchemaValidationException(
                SchemaEvolutionErrorCode.UNSUPPORTED_SCHEMA_CHANGE_TYPE,
                "Schema change type '%s' is not supported",
                tableIdentifier,
                jobId);
    }

    /** Create an exception for outdated schema events */
    public static SchemaValidationException outdatedEvent(
            TableIdentifier tableIdentifier, String jobId, long eventTime, long lastProcessedTime) {
        String message =
                String.format(
                        "Schema change event is outdated. Event time: %d, last processed: %d",
                        eventTime, lastProcessedTime);
        return new SchemaValidationException(
                SchemaEvolutionErrorCode.OUTDATED_SCHEMA_EVENT, message, tableIdentifier, jobId);
    }
}
