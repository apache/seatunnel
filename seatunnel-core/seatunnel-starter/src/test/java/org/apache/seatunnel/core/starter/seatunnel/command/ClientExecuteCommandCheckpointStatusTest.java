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

package org.apache.seatunnel.core.starter.seatunnel.command;

import org.apache.seatunnel.engine.core.checkpoint.CheckpointStatus;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.util.Locale;

/**
 * Verifies that the CLI enum arguments parsed by {@link ClientExecuteCommand} do not depend on the
 * JVM default locale.
 *
 * <p>The default locale is switched to Turkish, where {@code i} upper-cases to a dotted capital
 * {@code I} ({@code U+0130}), in {@link BeforeEach} and restored in {@link AfterEach}, so a failing
 * assertion cannot leak the locale into other tests sharing the JVM.
 */
@ResourceLock("java.util.Locale")
public class ClientExecuteCommandCheckpointStatusTest {

    private Locale base;
    private Locale display;
    private Locale format;

    @BeforeEach
    public void switchToTurkishLocale() {
        base = Locale.getDefault();
        display = Locale.getDefault(Locale.Category.DISPLAY);
        format = Locale.getDefault(Locale.Category.FORMAT);
        Locale.setDefault(new Locale("tr", "TR"));
    }

    @AfterEach
    public void restoreDefaultLocale() {
        Locale.setDefault(base);
        Locale.setDefault(Locale.Category.DISPLAY, display);
        Locale.setDefault(Locale.Category.FORMAT, format);
    }

    @Test
    public void testParseCheckpointStatusUnderTurkishLocale() {
        // "failed" and "in_progress" are the values that a bare toUpperCase() breaks under tr-TR;
        // the other two are here so the test pins the whole enum, not only today's default path.
        Assertions.assertEquals(
                CheckpointStatus.FAILED,
                ClientExecuteCommand.parseCheckpointStatus("failed"),
                "failed must map to FAILED under tr-TR, not throw on FAİLED");
        Assertions.assertEquals(
                CheckpointStatus.IN_PROGRESS,
                ClientExecuteCommand.parseCheckpointStatus("in_progress"),
                "in_progress must map to IN_PROGRESS under tr-TR");
        Assertions.assertEquals(
                CheckpointStatus.COMPLETED,
                ClientExecuteCommand.parseCheckpointStatus("Completed"));
        Assertions.assertEquals(
                CheckpointStatus.CANCELED, ClientExecuteCommand.parseCheckpointStatus("CANCELED"));

        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> ClientExecuteCommand.parseCheckpointStatus("finished"),
                "an unknown value must still be rejected");
    }
}
