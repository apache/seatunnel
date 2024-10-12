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

package org.apache.seatunnel.connectors.seatunnel.weaviate.exception;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;

public enum WeaviateConnectionErrorCode implements SeaTunnelErrorCode {
    SERVER_RESPONSE_FAILED("WEAVIATE-01", "WEAVIATE server response error"),
    WEAVIATE_CLASS_NOT_FOUND("WEAVIATE-02", "WEAVIATE Class not found"),
    WEAVIATE_PROPERTY_NOT_FOUND("WEAVIATE-03", "WEAVIATE Property not found"),
    NOT_SUPPORT_TYPE("WEAVIATE-04", "Type not support yet"),
    DATABASE_NO_CLASS("WEAVIATE-05", "Database no any Class"),
    SOURCE_TABLE_SCHEMA_IS_NULL("WEAVIATE-06", "Source table Class is null"),
    WEAVIATE_PROPERTY_IS_NULL("WEAVIATE-07", " WEAVIATE Property is null"),
    CREATE_CLASS_ERROR("WEAVIATE-08", "Create Class error"),
    ;

    private final String code;
    private final String description;

    WeaviateConnectionErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return code;
    }

    @Override
    public String getDescription() {
        return description;
    }
}
