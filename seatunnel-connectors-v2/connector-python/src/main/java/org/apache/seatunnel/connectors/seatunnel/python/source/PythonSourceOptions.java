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

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.Map;

/**
 * Config options for the Python source connector.
 *
 * <p>The Phase 1 MVP intentionally keeps the contract small: SeaTunnel launches one Python process,
 * passes one JSON config object through stdin, then parses stdout line by line as text.
 */
public class PythonSourceOptions {

    public static final String CONNECTOR_IDENTITY = "Python";

    /** Python interpreter or executable name used to launch the user script. */
    public static final Option<String> PYTHON_EXECUTABLE =
            Options.key("python.executable")
                    .stringType()
                    .defaultValue("python3")
                    .withDescription(
                            "Python interpreter used to launch the script. The resolved absolute path must be allowed by the server-side Python source policy");

    /** Script path that contains the user-defined data generation logic. */
    public static final Option<String> PYTHON_SCRIPT_PATH =
            Options.key("python.script.path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Python script path executed by the source connector");

    /** JSON-like config map serialized to the first stdin line for the Python process. */
    public static final Option<Map<String, Object>> PYTHON_SCRIPT_CONFIG =
            Options.key("python.script.config")
                    .mapObjectType()
                    .defaultValue(java.util.Collections.emptyMap())
                    .withDescription(
                            "Config map serialized as JSON and written to the first stdin line");

    /** Working directory used by ProcessBuilder before the Python process starts. */
    public static final Option<String> PYTHON_WORKING_DIRECTORY =
            Options.key("python.working.directory")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional working directory for the Python process. Defaults to the script parent directory");

    /** Output format used to parse stdout lines into SeaTunnelRow values. */
    public static final Option<String> FILE_FORMAT_TYPE =
            Options.key("file_format_type")
                    .stringType()
                    .defaultValue("text")
                    .withDescription(
                            "Stdout parsing format. Phase 1 supports only text output lines");

    /** Column delimiter for the text format output emitted by the Python script. */
    public static final Option<String> FIELD_DELIMITER =
            Options.key("field_delimiter")
                    .stringType()
                    .defaultValue(",")
                    .withDescription(
                            "Field delimiter for text format output. Only used when file_format_type=text");

    private PythonSourceOptions() {}
}
