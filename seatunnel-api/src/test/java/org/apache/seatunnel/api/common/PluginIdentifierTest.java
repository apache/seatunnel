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

package org.apache.seatunnel.api.common;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public class PluginIdentifierTest {

    @Test
    void testEqualsAndHashCodeAgreeUnderTurkishLocale() {
        Locale previous = Locale.getDefault();
        try {
            Locale.setDefault(new Locale("tr", "TR"));

            PluginIdentifier upper = PluginIdentifier.of("SEATUNNEL", "SOURCE", "ICEBERG");
            PluginIdentifier lower = PluginIdentifier.of("seatunnel", "source", "iceberg");

            Assertions.assertEquals(upper, lower);
            Assertions.assertEquals(upper.hashCode(), lower.hashCode());

            Map<PluginIdentifier, String> identifiers = new HashMap<>();
            identifiers.put(upper, "iceberg-source");
            Assertions.assertEquals("iceberg-source", identifiers.get(lower));
        } finally {
            Locale.setDefault(previous);
        }
    }
}
