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

package org.apache.seatunnel.connectors.sensorsdata.sdk.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.connectors.sensorsdata.format.config.SensorsDataOptions;

import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("checkstyle:MagicNumber")
public interface SensorsDataSDKSinkOptions extends SensorsDataOptions {

    Option<String> SERVER_URL =
            Options.key("server_url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Format：https://{ip}:8106/sa?project={project}");

    Option<Integer> BULK_SIZE =
            Options.key("bulk_size")
                    .intType()
                    .defaultValue(50)
                    .withDescription(
                            "Threshold for triggering flush operation. When the memory cache queue reaches this value, "
                                    + "the data in the cache will be batch uploaded.");

    Option<Integer> MAX_CACHE_ROW_SIZE =
            Options.key("max_cache_row_size")
                    .intType()
                    .defaultValue(0)
                    .withDescription(
                            "Maximum cache refresh size. If it exceeds this value, the flush operation will "
                                    + "be triggered immediately. The default value is 0, which depends on bulkSize.");

    Option<String> CONSUMER =
            Options.key("consumer")
                    .stringType()
                    .defaultValue("batch")
                    .withDescription("batch/console");

    Option<List<String>> INSTANT_EVENT_LIST =
            Options.key("instant_events")
                    .listType()
                    .defaultValue(new ArrayList<>())
                    .withDescription(
                            "Given a list of event names, mark the event as an instant event.");
}
