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

package org.apache.seatunnel.connectors.seatunnel.nebulagraph.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.List;

public final class NebulaGraphSinkOptions {

    public static final String CONNECTOR_IDENTITY = "NebulaGraph";

    public static final Option<List<String>> HOSTS =
            Options.key("hosts")
                    .listType()
                    .noDefaultValue()
                    .withDescription(
                            "NebulaGraph graphd addresses in host:port form, for example [\"graphd-1:9669\", \"graphd-2:9669\"].");

    public static final Option<String> USERNAME =
            Options.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("NebulaGraph username.");

    public static final Option<String> PASSWORD =
            Options.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("NebulaGraph password.");

    public static final Option<String> SPACE =
            Options.key("space")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Existing NebulaGraph space to write to.");

    public static final Option<String> TAG =
            Options.key("tag")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Existing NebulaGraph vertex tag to write to.");

    public static final Option<String> VID_FIELD =
            Options.key("vid_field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Input field used as the NebulaGraph vertex ID.");

    public static final Option<List<String>> WRITE_FIELDS =
            Options.key("write_fields")
                    .listType()
                    .noDefaultValue()
                    .withDescription(
                            "Input fields written as tag properties. By default all fields except vid_field are written.");

    public static final Option<NebulaGraphWriteMode> WRITE_MODE =
            Options.key("write_mode")
                    .enumType(NebulaGraphWriteMode.class)
                    .defaultValue(NebulaGraphWriteMode.INSERT)
                    .withDescription(
                            "Vertex write mode. INSERT uses INSERT VERTEX IF NOT EXISTS; UPDATE only changes an existing vertex.");

    public static final Option<Integer> BATCH_SIZE =
            Options.key("batch_size")
                    .intType()
                    .defaultValue(500)
                    .withDescription("Number of vertices buffered in each nGQL request.");

    public static final Option<Integer> TIMEOUT_MILLIS =
            Options.key("timeout_millis")
                    .intType()
                    .defaultValue(30000)
                    .withDescription(
                            "Connection, socket, and session wait timeout in milliseconds.");

    public static final Option<Integer> MAX_RETRIES =
            Options.key("max_retries")
                    .intType()
                    .defaultValue(0)
                    .withDescription(
                            "Retries after the initial write attempt. The default is 0 to avoid hidden re-execution after an ambiguous write result.");

    public static final Option<Integer> RETRY_INTERVAL_MILLIS =
            Options.key("retry_interval_millis")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("Delay between NebulaGraph client retries in milliseconds.");

    private NebulaGraphSinkOptions() {}
}
