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

package org.apache.seatunnel.connectors.seatunnel.tdengine.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class TDengineSourceOptions {

    public static final Option<String> URL =
            Options.key("url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The TDengine server URL, format: jdbc:TAOS-RS://host:port");

    public static final Option<String> USERNAME =
            Options.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The username for TDengine authentication");

    public static final Option<String> PASSWORD =
            Options.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The password for TDengine authentication");

    public static final Option<String> DATABASE =
            Options.key("database")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The TDengine database name");

    public static final Option<String> STABLE =
            Options.key("stable")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The super table name in TDengine");

    public static final Option<String> LOWER_BOUND =
            Options.key("lowerBound")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The lower bound for data query range");

    public static final Option<String> UPPER_BOUND =
            Options.key("upperBound")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The upper bound for data query range");
}
