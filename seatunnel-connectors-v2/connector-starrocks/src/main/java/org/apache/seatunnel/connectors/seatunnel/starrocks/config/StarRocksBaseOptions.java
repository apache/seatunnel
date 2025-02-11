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
package org.apache.seatunnel.connectors.seatunnel.starrocks.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.io.Serializable;
import java.util.List;

public class StarRocksBaseOptions implements Serializable {
    public static final String CONNECTOR_IDENTITY = "StarRocks";
    public static final Option<List<String>> NODE_URLS =
            Options.key("nodeUrls")
                    .listType()
                    .noDefaultValue()
                    .withDescription(
                            "StarRocks cluster address, the format is [\"fe_ip:fe_http_port\", ...]");

    public static final Option<String> DATABASE =
            Options.key("database")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The name of StarRocks database");

    public static final Option<String> TABLE =
            Options.key("table")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The name of StarRocks table");

    public static final Option<String> USERNAME =
            Options.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("StarRocks user username");

    public static final Option<String> PASSWORD =
            Options.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("StarRocks user password");
}
