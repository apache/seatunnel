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

package org.apache.seatunnel.connectors.seatunnel.tdengine.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.connectors.seatunnel.tdengine.config.TDengineSinkOptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class TDengineSinkFactoryTest {

    @Test
    void validRequiredOptions() {
        Assertions.assertDoesNotThrow(() -> validate(validConfig()));
    }

    @Test
    void requiredOptionsRejectMissingEmptyAndWhitespace() {
        for (String key : requiredKeys()) {
            Map<String, Object> missing = validConfig();
            missing.remove(key);
            Assertions.assertThrows(OptionValidationException.class, () -> validate(missing), key);

            for (String invalid : new String[] {"", "   "}) {
                Map<String, Object> config = validConfig();
                config.put(key, invalid);
                Assertions.assertThrows(
                        OptionValidationException.class, () -> validate(config), key);
            }
        }
    }

    private void validate(Map<String, Object> config) {
        ConfigValidator.of(ReadonlyConfig.fromMap(config))
                .validate(new TDengineSinkFactory().optionRule());
    }

    private String[] requiredKeys() {
        return new String[] {
            TDengineSinkOptions.URL.key(),
            TDengineSinkOptions.USERNAME.key(),
            TDengineSinkOptions.PASSWORD.key(),
            TDengineSinkOptions.DATABASE.key(),
            TDengineSinkOptions.STABLE.key()
        };
    }

    private Map<String, Object> validConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(TDengineSinkOptions.URL.key(), "jdbc:TAOS-RS://localhost:6041");
        config.put(TDengineSinkOptions.USERNAME.key(), "username");
        config.put(TDengineSinkOptions.PASSWORD.key(), "password");
        config.put(TDengineSinkOptions.DATABASE.key(), "database");
        config.put(TDengineSinkOptions.STABLE.key(), "stable");
        return config;
    }
}
