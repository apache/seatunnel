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
    ENUM_ILLEGAL("Common-01", "This enum is not supported by SeaTunnel"),
    FILE_OPERATION_ERROR("Common-02", "File operation error"),
    JSON_OPERATION_ERROR("Common-03", "Json covert/parse operation error"),
    REFLECT_CLASS_OPERATION_ERROR("Common-04", "Reflect class operation error"),
    SERIALIZE_OPERATION_ERROR("Common-05", "Serialize class operation error"),
    UNSUPPORTED_OPERATION("Common-06", "Unsupported operation error");

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
