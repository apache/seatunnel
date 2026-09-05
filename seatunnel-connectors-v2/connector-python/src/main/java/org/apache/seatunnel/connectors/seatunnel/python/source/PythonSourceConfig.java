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

package org.apache.seatunnel.connectors.seatunnel.python.source;

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import java.io.Serializable;
import java.util.Map;

/**
 * Immutable runtime config used by {@link PythonSourceReader}.
 *
 * <p>Validation is done here so the connector fails fast before spawning a process with an
 * unsupported parsing mode.
 */
public class PythonSourceConfig implements Serializable {

    private static final String SUPPORTED_FILE_FORMAT = "text";

    private final String pythonExecutable;
    private final String pythonScriptPath;
    private final Map<String, Object> pythonScriptConfig;
    private final String pythonWorkingDirectory;
    private final String fileFormatType;
    private final String fieldDelimiter;

    public PythonSourceConfig(ReadonlyConfig pluginConfig) {
        this.pythonExecutable = pluginConfig.get(PythonSourceOptions.PYTHON_EXECUTABLE);
        this.pythonScriptPath = pluginConfig.get(PythonSourceOptions.PYTHON_SCRIPT_PATH);
        this.pythonScriptConfig = pluginConfig.get(PythonSourceOptions.PYTHON_SCRIPT_CONFIG);
        this.pythonWorkingDirectory =
                pluginConfig.get(PythonSourceOptions.PYTHON_WORKING_DIRECTORY);
        this.fileFormatType = pluginConfig.get(PythonSourceOptions.FILE_FORMAT_TYPE);
        this.fieldDelimiter = pluginConfig.get(PythonSourceOptions.FIELD_DELIMITER);

        validate();
    }

    public String getPythonExecutable() {
        return pythonExecutable;
    }

    public String getPythonScriptPath() {
        return pythonScriptPath;
    }

    public Map<String, Object> getPythonScriptConfig() {
        return pythonScriptConfig;
    }

    public String getPythonWorkingDirectory() {
        return pythonWorkingDirectory;
    }

    public String getFileFormatType() {
        return fileFormatType;
    }

    public String getFieldDelimiter() {
        return fieldDelimiter;
    }

    /**
     * Phase 1 keeps the stdout contract intentionally narrow so the first implementation can stay
     * compatible with the existing text deserializer.
     */
    private void validate() {
        if (StringUtils.isBlank(pythonExecutable)) {
            throw new IllegalArgumentException("python.executable must not be blank");
        }
        if (StringUtils.isBlank(pythonScriptPath)) {
            throw new IllegalArgumentException("python.script.path must not be blank");
        }
        if (!SUPPORTED_FILE_FORMAT.equalsIgnoreCase(fileFormatType)) {
            throw new IllegalArgumentException(
                    "Unsupported file_format_type: "
                            + fileFormatType
                            + ". Phase 1 supports only text");
        }
        if (fieldDelimiter == null) {
            throw new IllegalArgumentException("field_delimiter must not be null");
        }
    }
}
