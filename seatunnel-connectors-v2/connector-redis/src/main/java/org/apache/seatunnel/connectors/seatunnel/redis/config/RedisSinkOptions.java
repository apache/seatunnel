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

public class RedisSinkOptions extends RedisBaseOptions {

    public static final Option<Long> EXPIRE =
            Options.key("expire")
                    .longType()
                    .defaultValue(-1L)
                    .withDescription("Set redis expiration time.");

    public static final Option<Boolean> SUPPORT_CUSTOM_KEY =
            Options.key("support_custom_key")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "if true, the key can be customized by the field value in the upstream data.");
    public static final Option<String> VALUE_FIELD =
            Options.key("value_field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The field of value you want to write to redis, support string list set zset");
    public static final Option<String> HASH_KEY_FIELD =
            Options.key("hash_key_field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The field of hash key you want to write to redis");

    public static final Option<String> HASH_VALUE_FIELD =
            Options.key("hash_value_field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The field of hash value you want to write to redis");
}
