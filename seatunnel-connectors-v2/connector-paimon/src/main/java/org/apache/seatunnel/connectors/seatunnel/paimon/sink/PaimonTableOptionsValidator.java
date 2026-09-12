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

package org.apache.seatunnel.connectors.seatunnel.paimon.sink;

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.options.SinkConnectorCommonOptions;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;

import java.util.Collections;
import java.util.Map;

/**
 * Validates Paimon sink {@code table_options} before job submission.
 *
 * <p>Paimon accepts open CoreOptions keys; this validator only rejects blank keys or null values.
 * Unsupported option keys fail later when Paimon creates the table.
 */
public final class PaimonTableOptionsValidator {

    private PaimonTableOptionsValidator() {}

    public static void validate(ReadonlyConfig config, Map<String, String> tableOptions) {
        if (tableOptions == null || tableOptions.isEmpty()) {
            return;
        }
        for (Map.Entry<String, String> entry : tableOptions.entrySet()) {
            if (StringUtils.isBlank(entry.getKey())) {
                throw new SeaTunnelRuntimeException(
                        SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                        "table_options contains a blank property key for Paimon sink.");
            }
            if (entry.getValue() == null) {
                throw new SeaTunnelRuntimeException(
                        SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                        String.format(
                                "table_options property '%s' has null value for Paimon sink.",
                                entry.getKey()));
            }
        }
    }

    public static void validate(ReadonlyConfig config) {
        validate(
                config,
                config.getOptional(SinkConnectorCommonOptions.TABLE_OPTIONS)
                        .orElse(Collections.emptyMap()));
    }
}
