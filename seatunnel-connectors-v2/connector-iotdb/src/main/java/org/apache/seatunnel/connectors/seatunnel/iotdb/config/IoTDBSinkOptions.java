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

package org.apache.seatunnel.connectors.seatunnel.iotdb.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.List;

public class IoTDBSinkOptions extends IoTDBCommonOptions {

    private static final int DEFAULT_BATCH_SIZE = 1024;

    public static final Option<String> KEY_TIMESTAMP =
            Options.key("key_timestamp")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("key timestamp");
    public static final Option<String> KEY_DEVICE =
            Options.key("key_device").stringType().noDefaultValue().withDescription("key device");
    public static final Option<List<String>> KEY_MEASUREMENT_FIELDS =
            Options.key("key_measurement_fields")
                    .listType()
                    .noDefaultValue()
                    .withDescription("key measurement fields");
    public static final Option<String> STORAGE_GROUP =
            Options.key("storage_group")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("store group");
    public static final Option<Integer> BATCH_SIZE =
            Options.key("batch_size")
                    .intType()
                    .defaultValue(DEFAULT_BATCH_SIZE)
                    .withDescription("batch size");
    public static final Option<Integer> MAX_RETRIES =
            Options.key("max_retries").intType().noDefaultValue().withDescription("max retries");
    public static final Option<Integer> RETRY_BACKOFF_MULTIPLIER_MS =
            Options.key("retry_backoff_multiplier_ms")
                    .intType()
                    .noDefaultValue()
                    .withDescription("retry backoff multiplier ms ");
    public static final Option<Integer> MAX_RETRY_BACKOFF_MS =
            Options.key("max_retry_backoff_ms")
                    .intType()
                    .noDefaultValue()
                    .withDescription("max retry backoff ms ");
    public static final Option<Integer> DEFAULT_THRIFT_BUFFER_SIZE =
            Options.key("default_thrift_buffer_size")
                    .intType()
                    .noDefaultValue()
                    .withDescription("default thrift buffer size");
    public static final Option<Integer> MAX_THRIFT_FRAME_SIZE =
            Options.key("max_thrift_frame_size")
                    .intType()
                    .noDefaultValue()
                    .withDescription("max thrift frame size");
    public static final Option<String> ZONE_ID =
            Options.key("zone_id").stringType().noDefaultValue().withDescription("zone id");
    public static final Option<Boolean> ENABLE_RPC_COMPRESSION =
            Options.key("enable_rpc_compression")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription("enable rpc comm");
    public static final Option<Integer> CONNECTION_TIMEOUT_IN_MS =
            Options.key("connection_timeout_in_ms")
                    .intType()
                    .noDefaultValue()
                    .withDescription("connection timeout ms");
}
