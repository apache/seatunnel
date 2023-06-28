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

public class RedisConfig {

    public enum RedisMode {
        SINGLE,
        CLUSTER;
    }

    public enum HashKeyParseMode {
        ALL,
        KV;
    }

    public static final Option<String> HOST =
            Options.key("host")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("redis hostname or ip");

    public static final Option<String> PORT =
            Options.key("port").stringType().noDefaultValue().withDescription("redis port");

    public static final Option<String> AUTH =
            Options.key("auth")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "redis authentication password, you need it when you connect to an encrypted cluster");

    public static final Option<String> USER =
            Options.key("user")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "redis authentication user, you need it when you connect to an encrypted cluster");

    public static final Option<String> KEY_PATTERN =
            Options.key("keys")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "keys pattern, redis source connector support fuzzy key matching, user needs to ensure that the matched keys are the same type");

    public static final Option<String> KEY =
            Options.key("key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The value of key you want to write to redis.");

    public static final Option<String> DATA_TYPE =
            Options.key("data_type")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("redis data types, support key hash list set zset.");

    public static final Option<RedisConfig.Format> FORMAT =
            Options.key("format")
                    .enumType(RedisConfig.Format.class)
                    .defaultValue(RedisConfig.Format.JSON)
                    .withDescription(
                            "the format of upstream data, now only support json and text, default json.");

    public static final Option<RedisConfig.RedisMode> MODE =
            Options.key("mode")
                    .enumType(RedisConfig.RedisMode.class)
                    .defaultValue(RedisMode.SINGLE)
                    .withDescription(
                            "redis mode, support single or cluster, default value is single");

    public static final Option<String> NODES =
            Options.key("nodes")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "redis nodes information, used in cluster mode, must like as the following format: [host1:port1, host2:port2]");

    public static final Option<RedisConfig.HashKeyParseMode> HASH_KEY_PARSE_MODE =
            Options.key("hash_key_parse_mode")
                    .enumType(RedisConfig.HashKeyParseMode.class)
                    .defaultValue(HashKeyParseMode.ALL)
                    .withDescription(
                            "hash key parse mode, support all or kv, default value is all");

    public static final Option<Long> EXPIRE =
            Options.key("expire")
                    .longType()
                    .defaultValue(-1L)
                    .withDescription("Set redis expiration time.");

    public enum Format {
        JSON,
        // TEXT will be supported later
    }
}
