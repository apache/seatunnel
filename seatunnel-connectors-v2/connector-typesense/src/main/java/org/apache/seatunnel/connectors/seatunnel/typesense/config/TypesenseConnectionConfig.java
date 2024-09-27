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

package org.apache.seatunnel.connectors.seatunnel.typesense.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.List;

public class TypesenseConnectionConfig {

    public static final Option<List<String>> HOSTS =
            Options.key("hosts")
                    .listType()
                    .noDefaultValue()
                    .withDescription(
                            "Typesense cluster http address, the format is host:port, allowing multiple hosts to be specified. Such as [\"host1:8018\", \"host2:8018\"]");

    public static final Option<String> APIKEY =
            Options.key("api_key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Typesense api key");

    public static final Option<String> PROTOCOL =
            Options.key("protocol")
                    .stringType()
                    .defaultValue("http")
                    .withDescription("Default is http , for Typesense Cloud use https");
}
