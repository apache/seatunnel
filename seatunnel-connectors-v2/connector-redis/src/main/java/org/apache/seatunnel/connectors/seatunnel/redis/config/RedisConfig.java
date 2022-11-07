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
        Options.key("host").stringType().noDefaultValue().withDescription("redis hostname or ip");

    public static final Option<String> PORT =
        Options.key("port").stringType().noDefaultValue().withDescription("redis port");

    public static final Option<String> AUTH = Options.key("auth").stringType().noDefaultValue().withDescription("auth");

    public static final Option<String> USER = Options.key("user").stringType().noDefaultValue().withDescription("user");

    public static final Option<String> KEY_PATTERN =
        Options.key("keys").stringType().noDefaultValue().withDescription("keys");

    public static final Option<String> KEY = Options.key("key").stringType().noDefaultValue().withDescription("key");

    public static final Option<String> DATA_TYPE =
        Options.key("data_type").stringType().noDefaultValue().withDescription("data_type");

    public static final Option<String> FORMAT =
        Options.key("format").stringType().noDefaultValue().withDescription("format");

    public static final Option<RedisConfig.RedisMode> MODE =
        Options.key("mode").enumType(RedisConfig.RedisMode.class).defaultValue(RedisMode.SINGLE)
            .withDescription("redis mode, support single or cluster, default value is single");

    public static final Option<String> NODES = Options.key("nodes").stringType().noDefaultValue()
        .withDescription("this parameter is required if mode is cluster");

    public static final Option<RedisConfig.HashKeyParseMode> HASH_KEY_PARSE_MODE =
        Options.key("hash_key_parse_mode").enumType(RedisConfig.HashKeyParseMode.class)
            .defaultValue(HashKeyParseMode.ALL).withDescription("hash key parse mode, support all or kv, default value is all");
}
