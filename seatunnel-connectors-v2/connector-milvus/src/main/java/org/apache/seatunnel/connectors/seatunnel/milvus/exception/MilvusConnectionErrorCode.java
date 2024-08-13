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

package org.apache.seatunnel.connectors.seatunnel.milvus.exception;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;

public enum MilvusConnectionErrorCode implements SeaTunnelErrorCode {
    SERVER_RESPONSE_FAILED("MILVUS-01", "Milvus server response error"),
    COLLECTION_NOT_FOUND("MILVUS-02", "Collection not found"),
    FIELD_NOT_FOUND("MILVUS-03", "Field not found"),
    DESC_COLLECTION_ERROR("MILVUS-04", "Desc collection error"),
    SHOW_COLLECTIONS_ERROR("MILVUS-05", "Show collections error"),
    COLLECTION_NOT_LOADED("MILVUS-06", "Collection not loaded"),
    NOT_SUPPORT_TYPE("MILVUS-07", "Type not support yet"),
    DATABASE_NO_COLLECTIONS("MILVUS-08", "Database no any collections"),
    SOURCE_TABLE_SCHEMA_IS_NULL("MILVUS-09", "Source table schema is null"),
    FIELD_IS_NULL("MILVUS-10", "Field is null"),
    CLOSE_CLIENT_ERROR("MILVUS-11", "Close client error"),
    DESC_INDEX_ERROR("MILVUS-12", "Desc index error"),
    CREATE_DATABASE_ERROR("MILVUS-13", "Create database error"),
    CREATE_COLLECTION_ERROR("MILVUS-14", "Create collection error"),
    CREATE_INDEX_ERROR("MILVUS-15", "Create index error"),
    ;

    private final String code;
    private final String description;

    MilvusConnectionErrorCode(String code, String description) {
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
