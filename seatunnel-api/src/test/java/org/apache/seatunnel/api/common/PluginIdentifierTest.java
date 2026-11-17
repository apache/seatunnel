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

import java.util.Locale;

public class PluginIdentifierTest {

    @Test
    public void testEqualsIgnoresCase() {
        Assertions.assertEquals(
                PluginIdentifier.of("SeaTunnel", "Source", "TiDB"),
                PluginIdentifier.of("seatunnel", "source", "tidb"));
        Assertions.assertNotEquals(
                PluginIdentifier.of("seatunnel", "source", "tidb"),
                PluginIdentifier.of("seatunnel", "source", "mysql"));
    }

    @Test
    public void testHashCodeIsIndependentOfDefaultLocale() {
        Locale defaultLocale = Locale.getDefault();
        try {
            // In a Turkish or Azerbaijani locale an upper-case "I" lower-cases to
            // a dotless "i", so a locale dependent hash code would differ from the
            // hash code of an equal identifier.
            Locale.setDefault(Locale.forLanguageTag("tr-TR"));
            PluginIdentifier identifier = PluginIdentifier.of("SeaTunnel", "Source", "Iceberg");
            PluginIdentifier sameIdentifier = PluginIdentifier.of("seatunnel", "source", "iceberg");
            Assertions.assertEquals(identifier, sameIdentifier);
            Assertions.assertEquals(identifier.hashCode(), sameIdentifier.hashCode());
        } finally {
            Locale.setDefault(defaultLocale);
        }
    }
}
