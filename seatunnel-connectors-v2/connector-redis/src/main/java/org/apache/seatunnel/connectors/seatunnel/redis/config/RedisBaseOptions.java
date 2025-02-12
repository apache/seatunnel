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

import java.util.List;

public class RedisBaseOptions {

    public static final String CONNECTOR_IDENTITY = "Redis";

    public enum RedisMode {
        SINGLE,
        CLUSTER;
    }

    public static final Option<String> HOST =
            Options.key("host")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("redis hostname or ip");

    public static final Option<Integer> PORT =
            Options.key("port").intType().noDefaultValue().withDescription("redis port");

    public static final Option<String> AUTH =
            Options.key("auth")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "redis authentication password, you need it when you connect to an encrypted cluster");

    public static final Option<Integer> DB_NUM =
            Options.key("db_num")
                    .intType()
                    .defaultValue(0)
                    .withDescription(
                            "Redis  database index id, it is connected to db 0 by default");

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

    public static final Option<RedisDataType> DATA_TYPE =
            Options.key("data_type")
                    .enumType(RedisDataType.class)
                    .noDefaultValue()
                    .withDescription("redis data types, support string hash list set zset.");

    public static final Option<RedisBaseOptions.Format> FORMAT =
            Options.key("format")
                    .enumType(RedisBaseOptions.Format.class)
                    .defaultValue(RedisBaseOptions.Format.JSON)
                    .withDescription(
                            "the format of upstream data, now only support json and text, default json.");

    public static final Option<RedisBaseOptions.RedisMode> MODE =
            Options.key("mode")
                    .enumType(RedisBaseOptions.RedisMode.class)
                    .defaultValue(RedisMode.SINGLE)
                    .withDescription(
                            "redis mode, support single or cluster, default value is single");

    public static final Option<List<String>> NODES =
            Options.key("nodes")
                    .listType()
                    .noDefaultValue()
                    .withDescription(
                            "redis nodes information, used in cluster mode, must like as the following format: [host1:port1, host2:port2]");

    public static final Option<Integer> BATCH_SIZE =
            Options.key("batch_size")
                    .intType()
                    .defaultValue(10)
                    .withDescription(
                            "batch_size is used to control the size of a batch of data during read and write operations"
                                    + ",default 10");

    public enum Format {
        JSON,
        // TEXT will be supported later
    }
}
