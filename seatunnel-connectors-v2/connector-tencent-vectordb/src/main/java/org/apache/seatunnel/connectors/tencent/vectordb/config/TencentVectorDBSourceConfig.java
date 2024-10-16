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

package org.apache.seatunnel.connectors.tencent.vectordb.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

public class TencentVectorDBSourceConfig {
    public static final String CONNECTOR_IDENTITY = "TencentVectorDB";

    public static final Option<String> URL =
            Options.key("url").stringType().noDefaultValue().withDescription("url");

    public static final Option<String> USER_NAME =
            Options.key("user_name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("user name for authentication");

    public static final Option<String> API_KEY =
            Options.key("api_key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("token for authentication");

    public static final Option<String> DATABASE =
            Options.key("database")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Tencent Vector DB database name");

    public static final Option<String> COLLECTION =
            Options.key("collection")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Tencent Vector DB collection name");
    public static final Option<Integer> BATCH_SIZE =
            Options.key("batch_size")
                    .intType()
                    .defaultValue(100)
                    .withDescription("Tencent Vector DB reader batch size");
}
