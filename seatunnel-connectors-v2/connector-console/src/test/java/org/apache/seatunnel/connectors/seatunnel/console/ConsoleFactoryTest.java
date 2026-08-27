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

package org.apache.seatunnel.connectors.seatunnel.console;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkOptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

public class ConsoleFactoryTest {

    private OptionRule sinkRule;

    @BeforeEach
    void setUp() {
        sinkRule = new ConsoleSinkFactory().optionRule();
    }

    @Test
    public void testOptionRule() {
        Assertions.assertNotNull(sinkRule);
    }

    @Test
    void testNonNegativeLogPrintDelay() {
        Assertions.assertDoesNotThrow(() -> validateLogPrintDelay(0));
        Assertions.assertDoesNotThrow(() -> validateLogPrintDelay(100));
    }

    @Test
    void testNegativeLogPrintDelayFails() {
        OptionValidationException exception =
                Assertions.assertThrows(
                        OptionValidationException.class, () -> validateLogPrintDelay(-1));

        Assertions.assertTrue(
                exception.getMessage().contains(ConsoleSinkOptions.LOG_PRINT_DELAY.key()));
    }

    private void validateLogPrintDelay(int delayMs) {
        Map<String, Object> config =
                Collections.singletonMap(ConsoleSinkOptions.LOG_PRINT_DELAY.key(), delayMs);
        ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(sinkRule);
    }
}
