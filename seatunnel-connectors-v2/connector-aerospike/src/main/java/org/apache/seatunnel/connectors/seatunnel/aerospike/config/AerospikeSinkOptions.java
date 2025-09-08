/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.aerospike.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import lombok.Getter;

import java.util.HashMap;
import java.util.Map;

@Getter
public class AerospikeSinkOptions {
    private final String host;
    private final int port;
    private final String namespace;
    private final String set;
    private final String username;
    private final String password;

    public AerospikeSinkOptions(ReadonlyConfig config) {
        this.host = config.get(HOST);
        this.port = config.get(PORT);
        this.namespace = config.get(NAMESPACE);
        this.set = config.get(SET);
        this.username = config.get(USERNAME);
        this.password = config.get(PASSWORD);
    }

    public static final Option<String> HOST =
            Options.key("host").stringType().noDefaultValue().withDescription("The aerospike host");

    public static final Option<Integer> PORT =
            Options.key("port").intType().defaultValue(3000).withDescription("The aerospike port");

    public static final Option<String> NAMESPACE =
            Options.key("namespace")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The aerospike namespace");

    public static final Option<String> SET =
            Options.key("set")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The aerospike set name");

    public static final Option<String> USERNAME =
            Options.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The username for Aerospike");

    public static final Option<String> PASSWORD =
            Options.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The password for Aerospike");

    public static final Option<String> KEY_FIELD =
            Options.key("key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The field used as Aerospike key");

    public static final Option<String> BIN_NAME =
            Options.key("bin_name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The bin name for storing data");

    public static final Option<String> DATA_FORMAT =
            Options.key("data_format")
                    .stringType()
                    .defaultValue("string")
                    .withDescription("Data format: map/string/kv");

    public static final Option<Integer> WRITE_TIMEOUT =
            Options.key("write_timeout")
                    .intType()
                    .defaultValue(200)
                    .withDescription("Write timeout in milliseconds");

    public static final Option<Map<String, String>> FIELD_TYPES =
            Options.key("schema.field")
                    .mapType()
                    .defaultValue(new HashMap<>())
                    .withDescription(
                            "Fields to be written with their Aerospike data types. Example:  \"schema\": {\n"
                                    + "        \"field\": {\n"
                                    + "          \"name\": \"STRING\"\n"
                                    + "        }\n"
                                    + "      }");
}
