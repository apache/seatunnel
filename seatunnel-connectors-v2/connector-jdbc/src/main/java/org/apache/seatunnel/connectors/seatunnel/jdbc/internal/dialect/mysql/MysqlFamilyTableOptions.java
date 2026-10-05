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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.mysql;

import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/** Shared {@code table_options} validation for MySQL-compatible JDBC dialects. */
public final class MysqlFamilyTableOptions {

    public static final Set<String> SUPPORTED_KEYS =
            Collections.unmodifiableSet(
                    new LinkedHashSet<>(Arrays.asList("engine", "charset", "collate")));

    private MysqlFamilyTableOptions() {}

    public static void validate(String dialectName, Map<String, String> tableOptions) {
        if (tableOptions == null || tableOptions.isEmpty()) {
            return;
        }

        Set<String> unsupportedOptions = new LinkedHashSet<>(tableOptions.keySet());
        unsupportedOptions.removeAll(SUPPORTED_KEYS);
        if (!unsupportedOptions.isEmpty()) {
            throw new JdbcConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "Unsupported JDBC table_options for dialect '%s': %s. Supported keys: %s",
                            dialectName,
                            String.join(", ", unsupportedOptions),
                            String.join(", ", SUPPORTED_KEYS)));
        }
    }
}
