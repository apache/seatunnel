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
 * Exception thrown when sink writer schema operations fail. This includes schema application
 * failures, flush failures, and rollback issues.
 */
public class SinkWriterSchemaException extends SchemaEvolutionException {

    public SinkWriterSchemaException(SchemaEvolutionErrorCode errorCode, String errorMessage) {
        super(errorCode, errorMessage);
    }

    public SinkWriterSchemaException(
            SchemaEvolutionErrorCode errorCode, String errorMessage, Throwable cause) {
        super(errorCode, errorMessage, cause);
    }

    public SinkWriterSchemaException(
            SchemaEvolutionErrorCode errorCode,
            String errorMessage,
            TableIdentifier tableIdentifier,
            String jobId) {
        super(errorCode, errorMessage, tableIdentifier, jobId);
    }

    public SinkWriterSchemaException(
            SchemaEvolutionErrorCode errorCode,
            String errorMessage,
            TableIdentifier tableIdentifier,
            String jobId,
            Throwable cause) {
        super(errorCode, errorMessage, tableIdentifier, jobId, cause);
    }

    /** Create an exception for schema application failures */
    public static SinkWriterSchemaException applicationFailed(
            TableIdentifier tableIdentifier, String jobId, String reason, Throwable cause) {
        String message = String.format("Failed to apply schema change: %s", reason);
        return new SinkWriterSchemaException(
                SchemaEvolutionErrorCode.SCHEMA_CHANGE_APPLICATION_FAILED,
                message,
                tableIdentifier,
                jobId,
                cause);
    }

    /** Create an exception for flush operation failures */
    public static SinkWriterSchemaException flushFailed(
            TableIdentifier tableIdentifier, String jobId, String reason, Throwable cause) {
        String message =
                String.format("Flush operation failed during schema evolution: %s", reason);
        return new SinkWriterSchemaException(
                SchemaEvolutionErrorCode.FLUSH_OPERATION_FAILED,
                message,
                tableIdentifier,
                jobId,
                cause);
    }
}
