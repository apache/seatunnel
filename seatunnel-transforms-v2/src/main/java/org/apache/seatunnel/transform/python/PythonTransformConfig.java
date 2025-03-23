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
import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.transform.common.ErrorHandleWay;

public class PythonTransformConfig {

    public static final Option<String> SOURCE_CODE =
            Options.key("source_code")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("source_code to compile");

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

    public PythonTransformConfig(ErrorHandleWay errorHandleWay) {
        this.errorHandleWay = errorHandleWay;
    }

    public static PythonTransformConfig of(ReadonlyConfig config) {
        return null;
    }
}
