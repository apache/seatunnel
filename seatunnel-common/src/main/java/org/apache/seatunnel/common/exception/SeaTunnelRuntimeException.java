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

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.Map;

/** SeaTunnel global exception, used to tell user more clearly error messages */
public class SeaTunnelRuntimeException extends RuntimeException {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final SeaTunnelErrorCode seaTunnelErrorCode;
    private final Map<String, String> params;

    public SeaTunnelRuntimeException(SeaTunnelErrorCode seaTunnelErrorCode, String errorMessage) {
        super(seaTunnelErrorCode.getErrorMessage() + " - " + errorMessage);
        this.seaTunnelErrorCode = seaTunnelErrorCode;
        this.params = new HashMap<>();
        ExceptionParamsUtil.assertParamsMatchWithDescription(
                seaTunnelErrorCode.getDescription(), params);
    }

    public SeaTunnelRuntimeException(
            SeaTunnelErrorCode seaTunnelErrorCode, String errorMessage, Throwable cause) {
        super(seaTunnelErrorCode.getErrorMessage() + " - " + errorMessage, cause);
        this.seaTunnelErrorCode = seaTunnelErrorCode;
        this.params = new HashMap<>();
        ExceptionParamsUtil.assertParamsMatchWithDescription(
                seaTunnelErrorCode.getDescription(), params);
    }

    public SeaTunnelRuntimeException(SeaTunnelErrorCode seaTunnelErrorCode, Throwable cause) {
        super(seaTunnelErrorCode.getErrorMessage(), cause);
        this.seaTunnelErrorCode = seaTunnelErrorCode;
        this.params = new HashMap<>();
        ExceptionParamsUtil.assertParamsMatchWithDescription(
                seaTunnelErrorCode.getDescription(), params);
    }

    public SeaTunnelRuntimeException(
            SeaTunnelErrorCode seaTunnelErrorCode, Map<String, String> params) {
        super(ExceptionParamsUtil.getDescription(seaTunnelErrorCode.getErrorMessage(), params));
        this.seaTunnelErrorCode = seaTunnelErrorCode;
        this.params = params;
    }

    public SeaTunnelRuntimeException(
            SeaTunnelErrorCode seaTunnelErrorCode, Map<String, String> params, Throwable cause) {
        super(
                ExceptionParamsUtil.getDescription(seaTunnelErrorCode.getErrorMessage(), params),
                cause);
        this.seaTunnelErrorCode = seaTunnelErrorCode;
        this.params = params;
    }

    public SeaTunnelErrorCode getSeaTunnelErrorCode() {
        return seaTunnelErrorCode;
    }

    public Map<String, String> getParams() {
        return params;
    }

    public Map<String, String> getParamsValueAsMap(String key) {
        try {
            return OBJECT_MAPPER.readValue(
                    params.get(key), new TypeReference<Map<String, String>>() {});
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public <T> T getParamsValueAs(String key) {
        try {
            return OBJECT_MAPPER.readValue(params.get(key), new TypeReference<T>() {});
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
