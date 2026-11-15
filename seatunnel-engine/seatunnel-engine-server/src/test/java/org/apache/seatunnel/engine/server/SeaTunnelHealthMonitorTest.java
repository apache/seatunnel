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

package org.apache.seatunnel.engine.server;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Locale;

public class SeaTunnelHealthMonitorTest {

    /**
     * Verifies that the percentage formatter always uses {@link Locale#ROOT} so the rendered output
     * is stable regardless of the JVM default locale. Some locales (e.g. {@code de_DE}) would
     * otherwise produce {@code 12,34} instead of {@code 12.34}.
     */
    @Test
    public void testPercentageStringIsLocaleStable() throws Exception {
        Method method =
                SeaTunnelHealthMonitor.class.getDeclaredMethod("percentageString", double.class);
        method.setAccessible(true);

        Locale previous = Locale.getDefault();
        try {
            Locale.setDefault(Locale.GERMANY);
            String result = (String) method.invoke(null, 12.345);
            Assertions.assertEquals("12.35%", result);
        } finally {
            Locale.setDefault(previous);
        }
    }

    /** Verifies that the number-to-unit formatter always uses {@link Locale#ROOT}. */
    @Test
    public void testNumberToUnitIsLocaleStable() throws Exception {
        Method method = SeaTunnelHealthMonitor.class.getDeclaredMethod("numberToUnit", long.class);
        method.setAccessible(true);

        Locale previous = Locale.getDefault();
        try {
            Locale.setDefault(Locale.GERMANY);
            String result = (String) method.invoke(null, 2L * 1024 * 1024);
            Assertions.assertEquals("2.0M", result);
        } finally {
            Locale.setDefault(previous);
        }
    }
}
