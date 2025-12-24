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

package org.apache.seatunnel.connectors.seatunnel.typesense.exception;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;

public enum TypesenseConnectorErrorCode implements SeaTunnelErrorCode {
    QUERY_PARAM_ERROR("TYPESENSE-01", "Query parameter error"),
    QUERY_COLLECTION_EXISTS_ERROR("TYPESENSE-02", "Whether the collection stores query exceptions"),
    QUERY_COLLECTION_LIST_ERROR("TYPESENSE-03", "Collection list acquisition exception"),
    FIELD_TYPE_MAPPING_ERROR("TYPESENSE-04", "Failed to obtain the field"),
    CREATE_COLLECTION_ERROR("TYPESENSE-05", "Create collection failed"),
    DROP_COLLECTION_ERROR("TYPESENSE-06", "Drop collection failed"),
    TRUNCATE_COLLECTION_ERROR("TYPESENSE-07", "Truncate collection failed"),
    QUERY_COLLECTION_NUM_ERROR("TYPESENSE-08", "Query collection doc number failed"),
    INSERT_DOC_ERROR("TYPESENSE-09", "Insert documents failed"),
    DELETE_COLLECTION_ERROR("TYPESENSE-10", "Truncate collection failed");
    private final String code;
    private final String description;

    TypesenseConnectorErrorCode(String code, String description) {
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
