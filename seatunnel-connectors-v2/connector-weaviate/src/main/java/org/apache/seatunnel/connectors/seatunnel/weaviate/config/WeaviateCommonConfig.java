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

package org.apache.seatunnel.connectors.seatunnel.weaviate.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

public abstract class WeaviateCommonConfig {

    public static final String CONNECTOR_IDENTITY = "Weaviate";

    public static final Option<String> URL =
            Options.key("url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Weaviate public endpoint");

    public static final Option<String> CLASS_NAME =
            Options.key("class_name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Weaviate class name , tart class names with an upper case letter.");

    public static final Option<String> KEY =
            Options.key("key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Weaviate api key for authentication");

    // connectionTimeout
    public static final Option<Integer> CONNECTION_TIMEOUT =
            Options.key("connection_timeout")
                    .intType()
                    .defaultValue(60)
                    .withDescription("Connection timeout");

    // connectionRequestTimeout
    public static final Option<Integer> CONNECTION_REQUEST_TIMEOUT =
            Options.key("connection_request_timeout")
                    .intType()
                    .defaultValue(60)
                    .withDescription("Connection request timeout");

    // socketTimeout
    public static final Option<Integer> SOCKET_TIMEOUT =
            Options.key("socket_timeout")
                    .intType()
                    .defaultValue(60)
                    .withDescription("Socket timeout");

    public static final Option<String> VECTOR_FIELD =
            Options.key("vector_field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Vector field");
}
