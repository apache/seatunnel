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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConditionExtension;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;

import java.util.Map;

/**
 * Early validation for Paimon sink {@code table_options}. Delegates to {@link
 * PaimonTableOptionsValidator}. Options apply only on SaveMode schema auto-create and are not
 * merged into runtime {@code paimon.table.write-props}.
 */
public class PaimonTableOptionsConditionExtension
        implements ConditionExtension<Map<String, String>> {

    public static final PaimonTableOptionsConditionExtension INSTANCE =
            new PaimonTableOptionsConditionExtension();

    private PaimonTableOptionsConditionExtension() {}

    @Override
    public String description() {
        return "must use non-blank keys and non-null values; applied only to SaveMode auto-create"
                + " schema options (not merged into runtime paimon.table.write-props; see Paimon"
                + " connector docs)";
    }

    @Override
    public boolean evaluate(ReadonlyConfig config, Map<String, String> value)
            throws OptionValidationException {
        if (value == null || value.isEmpty()) {
            return true;
        }
        try {
            PaimonTableOptionsValidator.validate(config, value);
            return true;
        } catch (SeaTunnelRuntimeException e) {
            throw new OptionValidationException(e.getMessage());
        }
    }
}
