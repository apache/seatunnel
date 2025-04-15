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

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

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

    public static final Option<List<Map<String, Object>>> TABLE_LIST =
            Options.key("table_list")
                    .type(new TypeReference<List<Map<String, Object>>>() {})
                    .noDefaultValue()
                    .withDescription("List of tables to be written to MaxCompute.");

    public static final Option<String> TABLE_NAME =
            Options.key("table_name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Target Maxcompute table name eg: fake");

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
}
