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

package org.apache.seatunnel.connectors.seatunnel.lance.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.HashMap;
import java.util.Map;

public class LanceSinkOptions extends LanceCommonOptions {

    public static final Option<Integer> WRITE_MAX_ROWS_PER_FILE =
            Options.key("lance.write.max-rows-per-file")
                    .intType()
                    .defaultValue(10)
                    .withDescription(
                            "lance dataset write params which specified max rows per file.");

    public static final Option<Integer> WRITE_MAX_ROWS_PER_GROUP =
            Options.key("lance.write.max-rows-per-group")
                    .intType()
                    .defaultValue(20)
                    .withDescription(
                            "lance dataset write params which specified max rows per group.");

    public static final Option<Long> WRITE_MAX_BYTES_PER_FILE =
            Options.key("lance.write.max-bytes-per-file")
                    .longType()
                    .defaultValue(2048 * 10L)
                    .withDescription(
                            "lance dataset write params which specified max bytes per file.");

    public static final Option<String> WRITE_MODE =
            Options.key("lance.write.mode")
                    .stringType()
                    .defaultValue("CREATE")
                    .withDescription("lance dataset write params which specified mode.");

    public static final Option<Boolean> WRITE_ENABLE_STABLE_ROW_IDS =
            Options.key("lance.write.enable.stable.row.ids")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "lance dataset write params which specified enable stable row ids.");

    public static final Option<Map<String, String>> WRITE_STORAGE_OPTIONS =
            Options.key("lance.write.storage.options")
                    .mapType()
                    .defaultValue(new HashMap<>())
                    .withDescription(
                            "lance dataset write params which specified storage options params.");
}
