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

package org.apache.seatunnel.format.avro.exception;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;

public enum AvroFormatErrorCode implements SeaTunnelErrorCode {
    UNSUPPORTED_DATA_TYPE("AVRO-01", "Unsupported data type."),
    SERIALIZATION_ERROR("AVRO-02", "serialize error."),
    FILED_NOT_EXIST("AVRO-03", "Field not exist.");

    private final String code;
    private final String description;

    AvroFormatErrorCode(String code, String description) {
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
