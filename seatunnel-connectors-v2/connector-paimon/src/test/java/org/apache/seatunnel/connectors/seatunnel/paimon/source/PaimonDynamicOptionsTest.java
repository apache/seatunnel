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

package org.apache.seatunnel.connectors.seatunnel.paimon.source;

import org.apache.seatunnel.connectors.seatunnel.paimon.source.converter.SqlToPaimonPredicateConverter;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PaimonDynamicOptionsTest {

    @Test
    public void testParseDynamicOptionsWithIncrementalTimestamp() {
        String query =
                "SELECT * FROM table /*+ OPTIONS('incremental-between-timestamp' = '2025-03-12 00:00:00,2025-03-12 00:08:00') */ WHERE int_col > 3";
        Map<String, String> dynamicOptions =
                SqlToPaimonPredicateConverter.parseDynamicOptions(query);
        assertEquals(1, dynamicOptions.size());
        assertTrue(dynamicOptions.containsKey("incremental-between-timestamp"));
        assertEquals(
                "2025-03-12 00:00:00,2025-03-12 00:08:00",
                dynamicOptions.get("incremental-between-timestamp"));
    }

    @Test
    public void testParseDynamicOptionsWithScanTag() {
        String query =
                "SELECT * FROM table /*+ OPTIONS('scan.tag-name' = 'my-tag') */ WHERE int_col > 3";
        Map<String, String> dynamicOptions =
                SqlToPaimonPredicateConverter.parseDynamicOptions(query);
        assertEquals(1, dynamicOptions.size());
        assertTrue(dynamicOptions.containsKey("scan.tag-name"));
        assertEquals("my-tag", dynamicOptions.get("scan.tag-name"));
    }

    @Test
    public void testParseDynamicOptionsWithMultipleOptions() {
        String query =
                "SELECT * FROM table /*+ OPTIONS('incremental-between-timestamp' = '2025-03-12 00:00:00,2025-03-12 00:08:00', 'scan.tag-name' = 'my-tag', 'scan.snapshot-id' = '123') */ WHERE int_col > 3";
        Map<String, String> dynamicOptions =
                SqlToPaimonPredicateConverter.parseDynamicOptions(query);
        assertEquals(3, dynamicOptions.size());
        assertTrue(dynamicOptions.containsKey("incremental-between-timestamp"));
        assertTrue(dynamicOptions.containsKey("scan.tag-name"));
        assertTrue(dynamicOptions.containsKey("scan.snapshot-id"));
        assertEquals(
                "2025-03-12 00:00:00,2025-03-12 00:08:00",
                dynamicOptions.get("incremental-between-timestamp"));
        assertEquals("my-tag", dynamicOptions.get("scan.tag-name"));
        assertEquals("123", dynamicOptions.get("scan.snapshot-id"));
    }

    @Test
    public void testParseDynamicOptionsWithNoOptions() {
        String query = "SELECT * FROM table WHERE int_col > 3";
        Map<String, String> dynamicOptions =
                SqlToPaimonPredicateConverter.parseDynamicOptions(query);
        assertTrue(dynamicOptions.isEmpty());
    }

    @Test
    public void testParseDynamicOptionsWithEmptyOptions() {
        String query = "SELECT * FROM table /*+ OPTIONS() */ WHERE int_col > 3";
        Map<String, String> dynamicOptions =
                SqlToPaimonPredicateConverter.parseDynamicOptions(query);
        assertTrue(dynamicOptions.isEmpty());
    }
}
