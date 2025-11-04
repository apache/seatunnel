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
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;

import lombok.Getter;

/**
 * Base exception class for schema evolution related errors. This exception provides detailed
 * context about schema evolution failures.
 */
@Getter
public class SchemaEvolutionException extends SeaTunnelRuntimeException {

    private final TableIdentifier tableIdentifier;

    private final String jobId;

    public SchemaEvolutionException(SchemaEvolutionErrorCode errorCode, String errorMessage) {
        super(errorCode, errorMessage);
        this.tableIdentifier = null;
        this.jobId = null;
    }

    public SchemaEvolutionException(
            SchemaEvolutionErrorCode errorCode, String errorMessage, Throwable cause) {
        super(errorCode, errorMessage, cause);
        this.tableIdentifier = null;
        this.jobId = null;
    }

    public SchemaEvolutionException(
            SchemaEvolutionErrorCode errorCode,
            String errorMessage,
            TableIdentifier tableIdentifier,
            String jobId) {
        super(errorCode, enrichErrorMessage(errorMessage, tableIdentifier, jobId));
        this.tableIdentifier = tableIdentifier;
        this.jobId = jobId;
    }

    public SchemaEvolutionException(
            SchemaEvolutionErrorCode errorCode,
            String errorMessage,
            TableIdentifier tableIdentifier,
            String jobId,
            Throwable cause) {
        super(errorCode, enrichErrorMessage(errorMessage, tableIdentifier, jobId), cause);
        this.tableIdentifier = tableIdentifier;
        this.jobId = jobId;
    }

    private static String enrichErrorMessage(
            String originalMessage, TableIdentifier tableIdentifier, String jobId) {
        StringBuilder message = new StringBuilder(originalMessage);

        if (tableIdentifier != null) {
            message.append(" [Table: ").append(tableIdentifier).append("]");
        }

        if (jobId != null) {
            message.append(" [Job: ").append(jobId).append("]");
        }

        return message.toString();
    }
}
