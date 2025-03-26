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


import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.transform.common.ErrorHandleWay;
import org.apache.seatunnel.transform.common.TransformCommonOptions;
import org.apache.seatunnel.transform.exception.TransformException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Optional;

import static org.apache.seatunnel.transform.python.PythonTransformErrorCode.LOAD_SOURCE_CODE_FROM_PATH_ERROR;
import static org.apache.seatunnel.transform.python.PythonTransformErrorCode.SOURCE_CODE_MISS_ERROR;

public class PythonTransformConfig {

    public static final Option<String> SOURCE_CODE =
            Options.key("source_code")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("source_code to compile");

    public static final Option<String> SOURCE_CODE_PATH =
            Options.key("source_code_path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("load code from the specified directory to compile");

    public static final Option<Integer> JAVA_SERVER_PORT =
            Options.key("java_server_port")
                    .intType()
                    .defaultValue(25333)
                    .withDescription("Java Server used to receive python code");


    public static final Option<Integer> PYTHON_SERVER_PORT =
            Options.key("python_server_port")
                    .intType()
                    .defaultValue(25334)
                    .withDescription("Python Server used to receive java code");

    @Getter
    private final ErrorHandleWay errorHandleWay;

    @Getter
    private final Integer javaServerPort;

    @Getter
    private final Integer pythonServerPort;
    @Getter
    private final String sourceCode;

    public PythonTransformConfig(ErrorHandleWay errorHandleWay,
                                 Integer javaServerPort,
                                 Integer pythonServerPort,
                                 String sourceCode) {
        this.errorHandleWay = errorHandleWay;
        this.javaServerPort = javaServerPort;
        this.pythonServerPort = pythonServerPort;
        this.sourceCode = sourceCode;
    }

    public static PythonTransformConfig of(ReadonlyConfig config) {
        Optional<String> codeOp = config.getOptional(SOURCE_CODE);
        String code = "";
        if (codeOp.isPresent()) {
            code = codeOp.get();
        }
        Optional<String> codePathOp = config.getOptional(SOURCE_CODE_PATH);
        if (codePathOp.isPresent()) {
            code = loadCodeFromPath(codePathOp.get());
        }
        if (StringUtils.isEmpty(code)) {
            throw new TransformException(SOURCE_CODE_MISS_ERROR, SOURCE_CODE_MISS_ERROR.getDescription());
        }
        Integer javaPort = config.get(JAVA_SERVER_PORT);
        Integer pythonPort = config.get(PYTHON_SERVER_PORT);
        ErrorHandleWay rowErrorHandleWay =
                config.get(TransformCommonOptions.ROW_ERROR_HANDLE_WAY_OPTION);
        return new PythonTransformConfig(rowErrorHandleWay, javaPort, pythonPort, code);
    }

    private static String loadCodeFromPath(String filePath) {
        try {
            // 读取整个文件内容到字符串
            String code = new String(Files.readAllBytes(Paths.get(filePath)));
            return code;
        } catch (IOException e) {
            // 处理可能发生的IO异常
            throw new TransformException(LOAD_SOURCE_CODE_FROM_PATH_ERROR,
                    LOAD_SOURCE_CODE_FROM_PATH_ERROR.getDescription() + e.getMessage());
        }
    }
}
