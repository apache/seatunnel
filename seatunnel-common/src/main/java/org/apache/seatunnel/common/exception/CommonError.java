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

import org.apache.seatunnel.common.constants.PluginType;

import java.util.HashMap;
import java.util.Map;

import static org.apache.seatunnel.common.exception.CommonErrorCode.CONVERT_TO_CONNECTOR_TYPE_ERROR;
import static org.apache.seatunnel.common.exception.CommonErrorCode.CONVERT_TO_CONNECTOR_TYPE_ERROR_SIMPLE;
import static org.apache.seatunnel.common.exception.CommonErrorCode.CONVERT_TO_SEATUNNEL_TYPE_ERROR;
import static org.apache.seatunnel.common.exception.CommonErrorCode.CONVERT_TO_SEATUNNEL_TYPE_ERROR_SIMPLE;
import static org.apache.seatunnel.common.exception.CommonErrorCode.UNSUPPORTED_DATA_TYPE;

/**
 * The common error of SeaTunnel. This is an alternative to {@link CommonErrorCodeDeprecated} and is
 * used to define non-bug errors or expected errors for all connectors and engines. We need to
 * define a corresponding enumeration type in {@link CommonErrorCode} to determine the output error
 * message format and content. Then define the corresponding method in {@link CommonError} to
 * construct the corresponding error instance.
 */
public class CommonError {

    public static SeaTunnelRuntimeException unsupportedDataType(
            String identifier, String dataType, String field) {
        Map<String, String> params = new HashMap<>();
        params.put("identifier", identifier);
        params.put("dataType", dataType);
        params.put("field", field);
        return new SeaTunnelRuntimeException(UNSUPPORTED_DATA_TYPE, params);
    }

    public static SeaTunnelRuntimeException convertToSeaTunnelTypeError(
            String connector, PluginType pluginType, String dataType, String field) {
        Map<String, String> params = new HashMap<>();
        params.put("connector", connector);
        params.put("type", pluginType.getType());
        params.put("dataType", dataType);
        params.put("field", field);
        return new SeaTunnelRuntimeException(CONVERT_TO_SEATUNNEL_TYPE_ERROR, params);
    }

    public static SeaTunnelRuntimeException convertToSeaTunnelTypeError(
            String identifier, String dataType, String field) {
        Map<String, String> params = new HashMap<>();
        params.put("identifier", identifier);
        params.put("dataType", dataType);
        params.put("field", field);
        return new SeaTunnelRuntimeException(CONVERT_TO_SEATUNNEL_TYPE_ERROR_SIMPLE, params);
    }

    public static SeaTunnelRuntimeException convertToConnectorTypeError(
            String connector, PluginType pluginType, String dataType, String field) {
        Map<String, String> params = new HashMap<>();
        params.put("connector", connector);
        params.put("type", pluginType.getType());
        params.put("dataType", dataType);
        params.put("field", field);
        return new SeaTunnelRuntimeException(CONVERT_TO_CONNECTOR_TYPE_ERROR, params);
    }

    public static SeaTunnelRuntimeException convertToConnectorTypeError(
            String identifier, String dataType, String field) {
        Map<String, String> params = new HashMap<>();
        params.put("identifier", identifier);
        params.put("dataType", dataType);
        params.put("field", field);
        return new SeaTunnelRuntimeException(CONVERT_TO_CONNECTOR_TYPE_ERROR_SIMPLE, params);
    }
}
