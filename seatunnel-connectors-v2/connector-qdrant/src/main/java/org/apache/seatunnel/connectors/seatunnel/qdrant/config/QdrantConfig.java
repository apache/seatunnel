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

package org.apache.seatunnel.connectors.seatunnel.qdrant.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

public class QdrantConfig {

    public static final String CONNECTOR_IDENTITY = "Qdrant";

    public static final Option<String> HOST =
            Options.key("host")
                    .stringType()
                    .defaultValue("localhost")
                    .withDescription("Qdrant gRPC host");

    public static final Option<Integer> PORT =
            Options.key("port").intType().defaultValue(6334).withDescription("Qdrant gRPC port");

    public static final Option<String> API_KEY =
            Options.key("api_key").stringType().defaultValue("").withDescription("Qdrant API key");

    public static final Option<String> COLLECTION_NAME =
            Options.key("collection_name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Qdrant collection name");

    public static final Option<Boolean> USE_TLS =
            Options.key("use_tls")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to use TLS");
}
