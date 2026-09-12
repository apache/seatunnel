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

package org.apache.seatunnel.transform.python;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;

/** Error codes raised by the Python transform runtime. */
public enum PythonTransformErrorCode implements SeaTunnelErrorCode {
    COLUMNS_MUST_NOT_EMPTY(
            "PYTHON_TRANSFORM_ERROR_CODE-01", "PythonTransform config columns must not be empty"),
    PYTHON_SCRIPT_MUST_BE_CONFIGURED(
            "PYTHON_TRANSFORM_ERROR_CODE-02",
            "PythonTransform requires exactly one of source_code or source_code_path"),
    DEST_FIELD_MUST_NOT_EMPTY(
            "PYTHON_TRANSFORM_ERROR_CODE-03",
            "PythonTransform columns.dest_field must not be empty"),
    LOAD_WORKER_SCRIPT_ERROR(
            "PYTHON_TRANSFORM_ERROR_CODE-04",
            "PythonTransform failed to prepare the worker bootstrap script"),
    START_PYTHON_PROCESS_ERROR(
            "PYTHON_TRANSFORM_ERROR_CODE-05", "PythonTransform failed to start the Python process"),
    INIT_PYTHON_PROCESS_ERROR(
            "PYTHON_TRANSFORM_ERROR_CODE-06",
            "PythonTransform failed to initialize the Python worker"),
    PYTHON_EXECUTION_ERROR(
            "PYTHON_TRANSFORM_ERROR_CODE-07",
            "PythonTransform execution failed for the current row"),
    INVALID_PYTHON_RESULT_ERROR(
            "PYTHON_TRANSFORM_ERROR_CODE-08",
            "PythonTransform returned a result shape that does not match the configured columns"),
    PYTHON_PROCESS_TERMINATED_ERROR(
            "PYTHON_TRANSFORM_ERROR_CODE-09",
            "PythonTransform Python worker terminated unexpectedly"),
    DUPLICATE_DEST_FIELD_ERROR(
            "PYTHON_TRANSFORM_ERROR_CODE-10",
            "PythonTransform columns.dest_field values must be unique"),
    UNSUPPORTED_ERROR_HANDLE_WAY(
            "PYTHON_TRANSFORM_ERROR_CODE-11",
            "PythonTransform only supports FAIL and SKIP row_error_handle_way values"),
    PYTHON_TRANSFORM_DISABLED(
            "PYTHON_TRANSFORM_ERROR_CODE-12",
            "PythonTransform is disabled by the server-side security policy"),
    PYTHON_EXECUTABLE_NOT_ALLOWED(
            "PYTHON_TRANSFORM_ERROR_CODE-13",
            "PythonTransform python_executable is rejected by the server-side allowlist");

    /** Stable error code exposed to users and logs. */
    private final String code;

    /** Human-readable description associated with the code. */
    private final String description;

    PythonTransformErrorCode(String code, String description) {
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
