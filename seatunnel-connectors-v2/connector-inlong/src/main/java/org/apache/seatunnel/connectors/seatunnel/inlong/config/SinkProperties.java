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

package org.apache.seatunnel.connectors.seatunnel.inlong.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.List;

public class SinkProperties {

    public static final String IDENTIFIER = "Inlong";

    /** The default field delimiter is “,” */
    public static final String DEFAULT_FIELD_DELIMITER = "|";

    public static final Option<String> FIELD_DELIMITER =
            Options.key("field_delimiter")
                    .stringType()
                    .defaultValue(DEFAULT_FIELD_DELIMITER)
                    .withDescription(
                            "Customize the field delimiter for data format.The default field_delimiter is ',' ");

    public static final Option<InlongSemantics> SEMANTICS =
            Options.key("semantics")
                    .enumType(InlongSemantics.class)
                    .defaultValue(InlongSemantics.AT_LEAST_ONCE)
                    .withDescription(
                            "If semantic is specified as EXACTLY_ONCE, the producer will write all messages in a transaction.");

    public static final Option<List<String>> PARTITION_KEY_FIELDS =
            Options.key("partition_key_fields")
                    .listType()
                    .noDefaultValue()
                    .withDescription(
                            "Configure which fields are used as the key of the pulsar message.");
    public static final Option<String> MANAGER_URL =
            Options.key("manager-url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("manager url provider for Inlong manager");
    public static final Option<String> GROUP_ID =
            Options.key("group-id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("groupId for Inlong");
    public static final Option<Boolean> ENABLE_AUTH =
            Options.key("enable-auth")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("if to enable auth for Inlong");
    public static final Option<String> STREAM_ID =
            Options.key("stream-id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("streamId for Inlong");
    public static final Option<String> SECRET_ID =
            Options.key("secret-id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("secretId for Inlong");
    public static final Option<String> SECRET_KEY =
            Options.key("secret-key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("secretKey for Inlong");

    public static final int DEFAULT_ASYNC_BUFFER_SIZE_KB = 200 * 1024;
    public static final Option<Integer> ASYNC_BUFFER_SIZE =
            Options.key("async-buffer-size")
                    .intType()
                    .defaultValue(DEFAULT_ASYNC_BUFFER_SIZE_KB)
                    .withDescription("async buffer size for Inlong sdk");

    public static final int DEFAULT_ACTIVE_CONNECT_NUM = 10;
    public static final Option<Integer> ACTIVE_CONNECT_NUM =
            Options.key("active-connect-num")
                    .intType()
                    .defaultValue(DEFAULT_ACTIVE_CONNECT_NUM)
                    .withDescription("active connect num for Inlong sdk");

    public static final int DEFAULT_REQUEST_TIMEOUT_MS = 60 * 1000;
    public static final Option<Integer> REQUEST_TIMEOUT =
            Options.key("request-timeout")
                    .intType()
                    .defaultValue(DEFAULT_REQUEST_TIMEOUT_MS)
                    .withDescription("request timeout for Inlong sdk");

    public static final int DEFAULT_THREAD_NUM = Runtime.getRuntime().availableProcessors();
    public static final Option<Integer> THREAD_NUM =
            Options.key("thread-num")
                    .intType()
                    .defaultValue(DEFAULT_THREAD_NUM)
                    .withDescription("thread num for Inlong sdk");

    public static final int DEFAULT_BATCH_SEND_LEN = 500 * 1024;
    public static final Option<Integer> BATCH_SEND_LEN =
            Options.key("batch-send-len")
                    .intType()
                    .defaultValue(DEFAULT_BATCH_SEND_LEN)
                    .withDescription("batch send length for Inlong sdk");
}
