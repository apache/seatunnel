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

package org.apache.seatunnel.api.table.coordinator;

import org.apache.seatunnel.api.table.catalog.CatalogTable;

import lombok.Getter;

/** used to return change response information */
@Getter
public class SchemaResponse {
    private final boolean success;
    private final String message;
    private final CatalogTable currentSchema;
    private final Long schemaVersion;

    private SchemaResponse(
            boolean success, String message, CatalogTable currentSchema, Long schemaVersion) {
        this.success = success;
        this.message = message;
        this.currentSchema = currentSchema;
        this.schemaVersion = schemaVersion;
    }

    public static SchemaResponse success(CatalogTable currentSchema, Long schemaVersion) {
        String message = "Schema change completed successfully, current version: " + schemaVersion;
        return new SchemaResponse(true, message, currentSchema, schemaVersion);
    }

    public static SchemaResponse failure(String errorMessage) {
        return new SchemaResponse(false, errorMessage, null, null);
    }

    @Deprecated
    public String getErrorMessage() {
        return message;
    }
}
