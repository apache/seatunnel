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

package org.apache.seatunnel.connectors.seatunnel.starrocks.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.options.SinkConnectorCommonOptions;

import java.util.Collections;
import java.util.Map;

/** Validates StarRocks sink {@code table_options} before job submission. */
public final class StarRocksTableOptionsValidator {

    private StarRocksTableOptionsValidator() {}

    public static void validate(ReadonlyConfig config, Map<String, String> tableOptions) {
        StarRocksSaveModeUtil.INSTANCE.validateTableOptions(config, tableOptions);
    }

    public static void validate(ReadonlyConfig config) {
        validate(
                config,
                config.getOptional(SinkConnectorCommonOptions.TABLE_OPTIONS)
                        .orElse(Collections.emptyMap()));
    }
}
