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

package org.apache.seatunnel.connectors.seatunnel.sensorsdata.sdk.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.format.sensorsdata.config.SensorsDataOptions;

import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("checkstyle:MagicNumber")
public interface SensorsDataSDKOptions extends SensorsDataOptions {

    Option<String> SERVER_URL =
            Options.key("server_url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("格式：https://{ip}:8106/sa?project={project}");

    Option<Integer> BULK_SIZE =
            Options.key("bulk_size")
                    .intType()
                    .defaultValue(50)
                    .withDescription("触发 flush 操作阈值，当内存缓存队列达到该值时，将缓存中的数据批量上报，默认 50");

    Option<Integer> MAX_CACHE_ROW_SIZE =
            Options.key("max_cache_row_size")
                    .intType()
                    .defaultValue(0)
                    .withDescription("最大缓存刷新大小，若超过该值，立即触发 flush 操作，默认为 0 ，根据 bulkSize 来进行判断");

    Option<String> CONSUMER =
            Options.key("consumer")
                    .stringType()
                    .defaultValue("batch")
                    .withDescription("batch/console");

    Option<List<String>> INSTANT_EVENT_LIST =
            Options.key("instant_events")
                    .listType()
                    .defaultValue(new ArrayList<>())
                    .withDescription("即时事件的事件名列表，默认为空");
}
