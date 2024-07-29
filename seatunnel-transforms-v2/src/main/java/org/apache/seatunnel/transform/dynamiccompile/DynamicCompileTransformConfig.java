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

package org.apache.seatunnel.transform.dynamiccompile;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
public class DynamicCompileTransformConfig implements Serializable {
    public static final Option<String> SOURCE_CODE =
            Options.key("source_code")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("source_code to compile");

    public static final Option<CompileLanguage> COMPILE_LANGUAGE =
            Options.key("compile_language")
                    .enumType(CompileLanguage.class)
                    .noDefaultValue()
                    .withDescription("compile language");

    public static final Option<String> ABSOLUTE_PATH =
            Options.key("absolute_path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("absolute_path");

    public static final Option<CompilePattern> COMPILE_PATTERN =
            Options.key("compile_pattern")
                    .enumType(CompilePattern.class)
                    .defaultValue(CompilePattern.SOURCE_CODE)
                    .withDescription("compile_pattern");
}
