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

package org.apache.seatunnel.common.exception;

public enum CommonErrorCode implements SeaTunnelErrorCode {
    ENUM_ILLEGAL("COMMON-01", "This enum is not supported by SeaTunnel"),
    FILE_OPERATION_FAILED("COMMON-02", "File operation failed, such as (read,list,write,move,copy,sync) etc..."),
    JSON_OPERATION_FAILED("COMMON-03", "Json covert/parse operation failed"),
    REFLECT_CLASS_OPERATION_FAILED("COMMON-04", "Reflect class operation failed"),
    SERIALIZE_OPERATION_ERROR("COMMON-05", "Serialize class operation failed"),
    UNSUPPORTED_OPERATION("COMMON-06", "Unsupported operation error"),
    ILLEGAL_ARGUMENT("COMMON-07", "Illegal argument"),
    UNSUPPORTED_DATA_TYPE("COMMON-08", "Unsupported data type"),
    SQL_OPERATION_FAILED("COMMON-09", "Sql operation failed, such as (execute,addBatch,close) etc..."),
    TABLE_SCHEMA_GET_FAILED("COMMON-10", "Get table schema from upstream data failed"),
    FLUSH_DATA_FAILED("COMMON-11", "Flush data to sink connector failed"),
    WRITER_OPERATION_FAILED("COMMON-12", "Sink writer operation failed, such as (open, close) etc..."),
    READER_OPERATION_FAILED("COMMON-13", "Source reader operation failed, such as (open, close) etc..."),
    HTTP_OPERATION_FAILED("COMMON-14", "Http operation failed, such as (open, close, response) etc...");

    private final String code;
    private final String description;

    CommonErrorCode(String code, String description) {
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
