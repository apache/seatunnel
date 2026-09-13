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

package org.apache.seatunnel.connectors.seatunnel.salesforce.sink;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.salesforce.config.SalesforceSinkConfig;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SalesforceRowSerializerTest {
    private Map<String, Object> options() {
        Map<String, Object> values = new HashMap<>();
        values.put("client_id", "client");
        values.put("client_secret", "secret");
        values.put("username", "user");
        values.put("password", "password");
        values.put("instance_url", "https://example.my.salesforce.com");
        values.put("object_name", "Account");
        values.put("external_id_field", "External_Id__c");
        return values;
    }

    private SalesforceSinkConfig config() {
        return new SalesforceSinkConfig(ReadonlyConfig.fromMap(options()));
    }

    @Test
    void serializesNativeValuesAndNullsWithoutStringifyingNumbers() {
        String[] names = {
            "External_Id__c",
            "Flag",
            "Count",
            "Amount",
            "Day",
            "Time",
            "Timestamp",
            "ZonedTimestamp",
            "Body",
            "Empty"
        };
        SeaTunnelDataType<?>[] types = {
            BasicType.STRING_TYPE,
            BasicType.BOOLEAN_TYPE,
            BasicType.LONG_TYPE,
            new DecimalType(38, 10),
            LocalTimeType.LOCAL_DATE_TYPE,
            LocalTimeType.LOCAL_TIME_TYPE,
            LocalTimeType.LOCAL_DATE_TIME_TYPE,
            LocalTimeType.OFFSET_DATE_TIME_TYPE,
            PrimitiveByteArrayType.INSTANCE,
            BasicType.STRING_TYPE
        };
        SalesforceRowSerializer serializer =
                new SalesforceRowSerializer(new SeaTunnelRowType(names, types), config());
        BigDecimal amount = new BigDecimal("12345678901234567890.1234567890");
        ObjectNode result =
                serializer.serialize(
                        new SeaTunnelRow(
                                new Object[] {
                                    "key",
                                    true,
                                    9007199254740993L,
                                    amount,
                                    LocalDate.of(2026, 9, 8),
                                    LocalTime.of(12, 3, 4, 123000000),
                                    LocalDateTime.of(2026, 9, 8, 12, 3, 4),
                                    OffsetDateTime.parse("2026-09-08T12:03:04+05:30"),
                                    new byte[] {0, 1, 2},
                                    null
                                }));
        assertTrue(result.path("Flag").isBoolean());
        assertEquals(9007199254740993L, result.path("Count").longValue());
        assertEquals(amount, result.path("Amount").decimalValue());
        assertEquals("2026-09-08", result.path("Day").asText());
        assertEquals("12:03:04.123Z", result.path("Time").asText());
        assertEquals("2026-09-08T12:03:04Z", result.path("Timestamp").asText());
        assertEquals("2026-09-08T12:03:04+05:30", result.path("ZonedTimestamp").asText());
        assertEquals("AAEC", result.path("Body").asText());
        assertTrue(result.path("Empty").isNull());
    }

    @Test
    void rejectsPrecisionLossAndNonFiniteNumbers() {
        SalesforceRowSerializer time = serializer(LocalTimeType.LOCAL_TIME_TYPE);
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        time.serialize(
                                new SeaTunnelRow(new Object[] {"key", LocalTime.of(12, 0, 0, 1)})));
        SalesforceRowSerializer timestamp = serializer(LocalTimeType.LOCAL_DATE_TIME_TYPE);
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        timestamp.serialize(
                                new SeaTunnelRow(
                                        new Object[] {
                                            "key", LocalDateTime.of(2026, 9, 8, 12, 0, 0, 1)
                                        })));
        SalesforceRowSerializer number = serializer(BasicType.DOUBLE_TYPE);
        for (double value :
                new double[] {Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY}) {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> number.serialize(new SeaTunnelRow(new Object[] {"key", value})));
        }
    }

    @Test
    void detachesMutableBinaryFieldsFromReusedInputRows() {
        SalesforceRowSerializer serializer = serializer(PrimitiveByteArrayType.INSTANCE);
        byte[] bytes = {0, 1, 2};
        ObjectNode result = serializer.serialize(new SeaTunnelRow(new Object[] {"key", bytes}));
        bytes[0] = 42;
        assertEquals("AAEC", result.path("Value").asText());
    }

    private SalesforceRowSerializer serializer(SeaTunnelDataType<?> type) {
        return new SalesforceRowSerializer(
                new SeaTunnelRowType(
                        new String[] {"External_Id__c", "Value"},
                        new SeaTunnelDataType[] {BasicType.STRING_TYPE, type}),
                config());
    }

    @ParameterizedTest
    @ValueSource(strings = {"Id", "attributes", "external_id__c", "bad/field"})
    void rejectsReservedDuplicateAndInvalidSchemaFields(String field) {
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        new SalesforceRowSerializer(
                                new SeaTunnelRowType(
                                        new String[] {"External_Id__c", field},
                                        new SeaTunnelDataType[] {
                                            BasicType.STRING_TYPE, BasicType.STRING_TYPE
                                        }),
                                config()));
    }

    @Test
    void requiresExternalIdAndRejectsNestedTypes() {
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        new SalesforceRowSerializer(
                                new SeaTunnelRowType(
                                        new String[] {"Name"},
                                        new SeaTunnelDataType[] {BasicType.STRING_TYPE}),
                                config()));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        serializer(
                                new SeaTunnelRowType(
                                        new String[] {"nested"},
                                        new SeaTunnelDataType[] {BasicType.STRING_TYPE})));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        new SalesforceRowSerializer(
                                new SeaTunnelRowType(
                                        new String[] {"External_Id__c"},
                                        new SeaTunnelDataType[] {BasicType.BOOLEAN_TYPE}),
                                config()));
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "ftp://host",
                "https://user:secret@host",
                "https://host/path",
                "https://host?token=secret",
                "https://host#secret",
                "invalid"
            })
    void rejectsUnsafeOrNonOriginUrls(String url) {
        Map<String, Object> values = options();
        values.put("instance_url", url);
        IllegalArgumentException error =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> new SalesforceSinkConfig(ReadonlyConfig.fromMap(values)));
        assertTrue(!error.getMessage().contains("secret"));
    }

    @Test
    void validatesBoundsBeforeOpeningConnections() {
        Object[][] invalid = {
            {"batch_size", 0},
            {"batch_size", 201},
            {"batch_max_bytes", 127},
            {"batch_max_bytes", 8388609},
            {"max_retries", -1},
            {"max_retries", 11},
            {"retry_interval_ms", -1L},
            {"retry_interval_ms", 60001L},
            {"request_timeout_ms", 0},
            {"object_name", "../Account"},
            {"external_id_field", "Id"},
            {"api_version", "v59/0"},
            {"client_secret", " "}
        };
        for (Object[] setting : invalid) {
            Map<String, Object> values = options();
            values.put((String) setting[0], setting[1]);
            assertThrows(
                    IllegalArgumentException.class,
                    () -> new SalesforceSinkConfig(ReadonlyConfig.fromMap(values)),
                    setting[0].toString());
        }
    }
}
