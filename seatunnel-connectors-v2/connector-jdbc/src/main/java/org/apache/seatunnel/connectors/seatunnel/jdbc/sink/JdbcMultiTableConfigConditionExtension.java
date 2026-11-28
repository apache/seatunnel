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

package org.apache.seatunnel.connectors.seatunnel.jdbc.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConditionExtension;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;

import java.util.Map;

/**
 * Early validation for the JDBC sink {@code multi_table_config} option.
 *
 * <p>Configuring {@code multi_table_config} is an explicit customization request, so it is only
 * meaningful together with a {@code primary_keys} mapping. A missing, empty or non-map {@code
 * primary_keys} is rejected during option-rule validation instead of being silently ignored while
 * the sink is created, which would fall back to the top-level {@code primary_keys} / catalog
 * primary key and write with an unexpected key.
 */
public class JdbcMultiTableConfigConditionExtension
        implements ConditionExtension<Map<String, Object>> {

    public static final JdbcMultiTableConfigConditionExtension INSTANCE =
            new JdbcMultiTableConfigConditionExtension();

    private static final String PRIMARY_KEYS = "primary_keys";

    private JdbcMultiTableConfigConditionExtension() {}

    @Override
    public String description() {
        return "must contain a non-empty primary_keys map when it is configured, for example "
                + "primary_keys { \"^t_.*$\" = [\"key1\"] }";
    }

    @Override
    public boolean evaluate(ReadonlyConfig config, Map<String, Object> value)
            throws OptionValidationException {
        // An absent or empty option means the user did not ask for per-table keys; keep the
        // existing fallback behavior in that case.
        if (value == null || value.isEmpty()) {
            return true;
        }
        Object primaryKeys = value.get(PRIMARY_KEYS);
        if (!(primaryKeys instanceof Map) || ((Map<?, ?>) primaryKeys).isEmpty()) {
            throw new OptionValidationException(
                    "multi_table_config is configured but multi_table_config.primary_keys is missing "
                            + "or empty. Configure at least one pattern-to-columns mapping under "
                            + "multi_table_config.primary_keys, or remove multi_table_config to keep "
                            + "the fallback behavior.");
        }
        return true;
    }
}
