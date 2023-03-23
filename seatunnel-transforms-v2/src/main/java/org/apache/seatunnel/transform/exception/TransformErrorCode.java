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

package org.apache.seatunnel.transform.exception;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;

public enum TransformErrorCode implements SeaTunnelErrorCode {
    INPUT_FIELD_NOT_FOUND(
            "FIELD_MAPPER_TRANSFORM-01", "field mapper input field not found in inputRowType"),

    FILTER_FIELD_NOT_FOUND("FILTER_FIELD_TRANSFORM-01", "filter filed not found"),

    SPLIT_OUTPUT_FIELD_EXISTS("SPLIT_TRANSFORM-01", "split output field exists"),

    MAPPER_FIELD_NOT_FOUND("FIELD_MAPPER_TRANSFORM-01", "mapper field not found");

    private final String code;
    private final String description;

    TransformErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.description;
    }
}
