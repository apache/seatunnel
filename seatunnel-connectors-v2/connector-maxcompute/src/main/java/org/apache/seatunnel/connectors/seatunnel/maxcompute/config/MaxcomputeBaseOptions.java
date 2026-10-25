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

package org.apache.seatunnel.connectors.seatunnel.maxcompute.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.io.Serializable;

public class MaxcomputeBaseOptions implements Serializable {

    public static final String PLUGIN_NAME = "Maxcompute";

    public static final Option<String> ACCESS_ID =
            Options.key("accessId")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Your Maxcompute accessId which cloud be access from Alibaba Cloud");
    public static final Option<String> ACCESS_KEY =
            Options.key("accesskey")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Your Maxcompute accessKey which cloud be access from Alibaba Cloud");
    public static final Option<String> STS_TOKEN =
            Options.key("sts_token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Your Maxcompute stsToken for temporary access");
    public static final Option<String> ENDPOINT =
            Options.key("endpoint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Your Maxcompute endpoint start with http");

    public static final Option<String> PROJECT =
            Options.key("project")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Your Maxcompute project which is created in Alibaba Cloud");

    public static final Option<String> TABLE_NAME =
            Options.key("table_name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Target Maxcompute table name eg: fake");

    public static final Option<String> SCHEMA_NAME =
            Options.key("schema_name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The MaxCompute Schema name (namespace between Project and Table). "
                                    + "Only required when the table resides in a non-default schema. "
                                    + "See https://www.alibabacloud.com/help/en/maxcompute/user-guide/schema-related-operations");

    public static final Option<String> PARTITION_SPEC =
            Options.key("partition_spec")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("This spec of Maxcompute partition table.");

    public static final Option<Integer> SPLIT_ROW =
            Options.key("split_row")
                    .intType()
                    .defaultValue(10000)
                    .withDescription("Number of rows per split. default: 10000");

    public static final Option<String> TUNNEL_ENDPOINT =
            Options.key("tunnel_endpoint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Tunnel endpoint, e.g. http://maxcompute:8080");

    public static final Option<String> TUNNEL_NAME =
            Options.key("tunnel_name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Tunnel quota name for exclusive resource groups");

    // ---- Odps REST client (control plane: metadata / schema / catalog) ----
    public static final Option<Long> CONNECT_TIMEOUT_MS =
            Options.key("connect_timeout_ms")
                    .longType()
                    .defaultValue(10000L)
                    .withDescription(
                            "HTTP connect timeout for the MaxCompute (ODPS) REST client "
                                    + "(metadata/catalog calls) in milliseconds. "
                                    + "Millisecond values are converted to whole seconds; "
                                    + "minimum 1000. Default 10000 (10s).");
    public static final Option<Long> READ_TIMEOUT_MS =
            Options.key("read_timeout_ms")
                    .longType()
                    .defaultValue(120000L)
                    .withDescription(
                            "HTTP read timeout for the MaxCompute (ODPS) REST client "
                                    + "(metadata/catalog calls) in milliseconds. "
                                    + "Millisecond values are converted to whole seconds; "
                                    + "minimum 1000. Default 120000 (120s).");
    public static final Option<Integer> RETRY_TIMES =
            Options.key("retry_times")
                    .intType()
                    .defaultValue(4)
                    .withDescription(
                            "Max retry times for the MaxCompute (ODPS) REST client. Default 4.");

    // ---- Tunnel client (data plane: bulk row read / write / upsert) ----
    public static final Option<Long> TUNNEL_CONNECT_TIMEOUT_MS =
            Options.key("tunnel_connect_timeout_ms")
                    .longType()
                    .defaultValue(180000L)
                    .withDescription(
                            "HTTP connect timeout for the MaxCompute Tunnel client "
                                    + "(data upload/download) in milliseconds. "
                                    + "Millisecond values are converted to whole seconds; "
                                    + "minimum 1000. Default 180000 (180s).");
    public static final Option<Long> TUNNEL_READ_TIMEOUT_MS =
            Options.key("tunnel_read_timeout_ms")
                    .longType()
                    .defaultValue(300000L)
                    .withDescription(
                            "HTTP read timeout for the MaxCompute Tunnel client "
                                    + "(data upload/download) in milliseconds. "
                                    + "Millisecond values are converted to whole seconds; "
                                    + "minimum 1000. Default 300000 (300s).");
    public static final Option<Integer> TUNNEL_RETRY_TIMES =
            Options.key("tunnel_retry_times")
                    .intType()
                    .defaultValue(4)
                    .withDescription(
                            "Max retry times for the MaxCompute Tunnel client. Default 4.");
}
