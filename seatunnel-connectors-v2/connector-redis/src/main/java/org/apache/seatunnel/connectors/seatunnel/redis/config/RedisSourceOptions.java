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

package org.apache.seatunnel.connectors.seatunnel.redis.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

public class RedisSourceOptions extends RedisBaseOptions {
    public enum HashKeyParseMode {
        ALL,
        KV;
    }

    public static final Option<HashKeyParseMode> HASH_KEY_PARSE_MODE =
            Options.key("hash_key_parse_mode")
                    .enumType(HashKeyParseMode.class)
                    .defaultValue(HashKeyParseMode.ALL)
                    .withDescription(
                            "hash key parse mode, support all or kv, default value is all");

    public static final Option<Boolean> READ_KEY_ENABLED =
            Options.key("read_key_enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "If set to true, the source connector reads Redis values along with their keys.");

    public static final Option<String> SINGLE_FIELD_NAME =
            Options.key("single_field_name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Specifies the field name to be used in the output row when reading single-value types "
                                    + "(e.g., string, list, zset).");

    public static final Option<String> KEY_FIELD_NAME =
            Options.key("key_field_name")
                    .stringType()
                    .defaultValue("key")
                    .withDescription("The value of key you want to write to redis.");
}
