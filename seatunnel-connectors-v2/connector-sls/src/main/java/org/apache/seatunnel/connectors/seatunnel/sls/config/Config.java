/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.sls.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import com.aliyun.openservices.log.common.Consts;

public class Config {
    public static final String CONNECTOR_IDENTITY = "Sls";

    public static final Option<String> ENDPOINT =
            Options.key("endpoint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Aliyun Access endpoint");
    public static final Option<String> PROJECT =
            Options.key("project")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Aliyun sls project");
    public static final Option<String> LOGSTORE =
            Options.key("logstore")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Aliyun sls logstore");
    public static final Option<String> ACCESS_KEY_ID =
            Options.key("access_key_id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Aliyun accessKey id");
    public static final Option<String> ACCESS_KEY_SECRET =
            Options.key("access_key_secret")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Aliyun accessKey secret");
    public static final Option<String> CONSUMER_GROUP =
            Options.key("consumer_group")
                    .stringType()
                    .defaultValue("SeaTunnel-Consumer-Group")
                    .withDescription("Aliyun sls consumer group");
    public static final Option<Integer> BATCH_SIZE =
            Options.key("batch_size")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("The amount of data pulled from sls each time");

    public static final Option<StartMode> START_MODE =
            Options.key("start_mode")
                    .objectType(StartMode.class)
                    .defaultValue(StartMode.GROUP_CURSOR)
                    .withDescription("initial consumption pattern of consumers");

    public static final Option<Consts.CursorMode> AUTO_CURSOR_RESET =
            Options.key("auto_cursor_reset")
                    .objectType(Consts.CursorMode.class)
                    .defaultValue(Consts.CursorMode.END)
                    .withDescription("init consumer cursor");

    public static final Option<Long> KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS =
            Options.key("partition-discovery.interval-millis")
                    .longType()
                    .defaultValue(-1L)
                    .withDescription(
                            "The interval for dynamically discovering topics and partitions.");
}
