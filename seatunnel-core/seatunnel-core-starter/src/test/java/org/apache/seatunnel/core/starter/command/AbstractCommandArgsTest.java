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

package org.apache.seatunnel.core.starter.command;

import org.apache.seatunnel.core.starter.command.AbstractCommandArgs.DryRunConverter;
import org.apache.seatunnel.core.starter.enums.DryRun;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AbstractCommandArgsTest {

    @Test
    public void testDryRunConverterWithValidStatic() {
        DryRunConverter converter = new DryRunConverter();

        Assertions.assertEquals(DryRun.STATIC, converter.convert("static"));
        Assertions.assertEquals(DryRun.STATIC, converter.convert("STATIC"));
    }

    @Test
    public void testDryRunConverterRejectsUnsupportedModes() {
        DryRunConverter converter = new DryRunConverter();

        IllegalArgumentException ex =
                Assertions.assertThrows(
                        IllegalArgumentException.class, () -> converter.convert("connect"));
        Assertions.assertTrue(
                ex.getMessage().contains("not implemented yet"), "Actual: " + ex.getMessage());

        Assertions.assertThrows(IllegalArgumentException.class, () -> converter.convert("sample"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> converter.convert("shadow"));
    }

    @Test
    public void testDryRunConverterRejectsInvalidMode() {
        DryRunConverter converter = new DryRunConverter();

        IllegalArgumentException ex =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> converter.convert("nonexistent_mode"));
        Assertions.assertTrue(
                ex.getMessage().contains("Currently only [static] is supported"),
                "Actual: " + ex.getMessage());
    }
}
