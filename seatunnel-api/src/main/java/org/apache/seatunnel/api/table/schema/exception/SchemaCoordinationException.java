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
 * Exception thrown when schema coordination operations fail. This includes timeout issues,
 * coordination conflicts, and coordinator state problems.
 */
public class SchemaCoordinationException extends SchemaEvolutionException {

    public SchemaCoordinationException(SchemaEvolutionErrorCode errorCode, String errorMessage) {
        super(errorCode, errorMessage);
    }

    public SchemaCoordinationException(
            SchemaEvolutionErrorCode errorCode, String errorMessage, Throwable cause) {
        super(errorCode, errorMessage, cause);
    }

    public SchemaCoordinationException(
            SchemaEvolutionErrorCode errorCode,
            String errorMessage,
            TableIdentifier tableIdentifier,
            String jobId) {
        super(errorCode, errorMessage, tableIdentifier, jobId);
    }

    public SchemaCoordinationException(
            SchemaEvolutionErrorCode errorCode,
            String errorMessage,
            TableIdentifier tableIdentifier,
            String jobId,
            Throwable cause) {
        super(errorCode, errorMessage, tableIdentifier, jobId, cause);
    }

    /** Create a timeout exception for schema changes */
    public static SchemaCoordinationException timeout(
            TableIdentifier tableIdentifier, String jobId, long timeoutSeconds, Throwable cause) {
        String message =
                String.format("Schema change operation timed out after %d seconds", timeoutSeconds);
        return new SchemaCoordinationException(
                SchemaEvolutionErrorCode.SCHEMA_CHANGE_TIMEOUT,
                message,
                tableIdentifier,
                jobId,
                cause);
    }

    /** Create an exception for schema change conflicts */
    public static SchemaCoordinationException conflict(
            TableIdentifier tableIdentifier, String currentJobId, String conflictingJobId) {
        String message =
                String.format(
                        "Schema change already in progress for table. Current job: %s, conflicting job: %s",
                        currentJobId, conflictingJobId);
        return new SchemaCoordinationException(
                SchemaEvolutionErrorCode.SCHEMA_CHANGE_ALREADY_IN_PROGRESS,
                message,
                tableIdentifier,
                currentJobId);
    }
}
