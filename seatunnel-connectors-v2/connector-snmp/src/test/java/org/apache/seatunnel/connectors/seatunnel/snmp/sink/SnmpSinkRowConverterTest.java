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

package org.apache.seatunnel.connectors.seatunnel.snmp.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.snmp.config.SnmpSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.snmp.exception.SnmpConnectorException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.snmp4j.smi.Counter32;
import org.snmp4j.smi.Counter64;
import org.snmp4j.smi.Gauge32;
import org.snmp4j.smi.Integer32;
import org.snmp4j.smi.IpAddress;
import org.snmp4j.smi.OID;
import org.snmp4j.smi.OctetString;
import org.snmp4j.smi.TimeTicks;
import org.snmp4j.smi.UnsignedInteger32;
import org.snmp4j.smi.Variable;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

class SnmpSinkRowConverterTest {

    @Test
    void testConvertsSourceCompatibleSchema() {
        SnmpSinkRowConverter converter = new SnmpSinkRowConverter(config(), sourceRowType());

        SnmpSetRequest request =
                converter.convert(
                        new SeaTunnelRow(
                                new Object[] {
                                    "127.0.0.1:161",
                                    ".1.3.6.1.2.1.1.5.0",
                                    "router-1",
                                    "OctetString",
                                    1234L
                                }));

        Assertions.assertEquals("1.3.6.1.2.1.1.5.0", request.getOid().toString());
        Assertions.assertEquals("router-1", request.getValue().toString());
        Assertions.assertInstanceOf(OctetString.class, request.getValue());
    }

    @Test
    void testSupportsDocumentedSmiTypes() {
        assertVariable("Integer32", "-42", Integer32.class, "-42");
        assertVariable("UnsignedInteger32", "4294967295", UnsignedInteger32.class, "4294967295");
        assertVariable("Counter32", "12", Counter32.class, "12");
        assertVariable("Gauge32", "13", Gauge32.class, "13");
        assertVariable("TimeTicks", "14", TimeTicks.class, "0:00:00.14");
        assertVariable(
                "Counter64", "18446744073709551615", Counter64.class, "18446744073709551615");
        assertVariable("OctetString", "router-1", OctetString.class, "router-1");

        Variable hex = SnmpSinkRowConverter.parseVariable("OctetStringHex", "00ff10");
        Assertions.assertInstanceOf(OctetString.class, hex);
        Assertions.assertArrayEquals(
                new byte[] {0x00, (byte) 0xff, 0x10}, ((OctetString) hex).getValue());

        assertVariable("OID", "1.3.6.1.2.1", OID.class, "1.3.6.1.2.1");
        assertVariable("IpAddress", "192.0.2.10", IpAddress.class, "192.0.2.10");
    }

    @Test
    void testSupportsSnmp4jSourceSyntaxStringsAndValues() {
        assertSourceVariable(new Integer32(-42), Integer32.class);
        assertSourceVariable(new UnsignedInteger32(11), Gauge32.class);
        assertSourceVariable(new Counter32(12), Counter32.class);
        assertSourceVariable(new Gauge32(13), Gauge32.class);
        assertSourceVariable(new TimeTicks(14), TimeTicks.class);
        assertSourceVariable(new TimeTicks(172_800_014L), TimeTicks.class);
        assertSourceVariable(new Counter64(15), Counter64.class);
        assertSourceVariable(new OctetString("router-1"), OctetString.class);
        assertSourceVariable(new OID("1.3.6.1.2.1"), OID.class);
        assertSourceVariable(new IpAddress("192.0.2.10"), IpAddress.class);
    }

    @Test
    void testPreservesOctetStringPayload() {
        SnmpSinkRowConverter converter = new SnmpSinkRowConverter(config(), sinkRowType());

        SnmpSetRequest whitespace =
                converter.convert(row("1.3.6.1.2.1.1.5.0", "  router-1  ", " OctetString "));
        Assertions.assertEquals("  router-1  ", whitespace.getValue().toString());

        SnmpSetRequest empty = converter.convert(row("1.3.6.1.2.1.1.5.0", "", "OctetString"));
        Assertions.assertEquals(0, ((OctetString) empty.getValue()).length());

        SnmpSetRequest utf8 =
                converter.convert(row("1.3.6.1.2.1.1.5.0", "router-\u03b1", "OctetString"));
        Assertions.assertArrayEquals(
                "router-\u03b1".getBytes(StandardCharsets.UTF_8),
                ((OctetString) utf8.getValue()).getValue());
    }

    @Test
    void testRejectsInvalidSchemaBeforeClientCreation() {
        SeaTunnelRowType missingField =
                new SeaTunnelRowType(
                        new String[] {"oid", "value"},
                        new SeaTunnelDataType[] {BasicType.STRING_TYPE, BasicType.STRING_TYPE});
        SnmpConnectorException missing =
                Assertions.assertThrows(
                        SnmpConnectorException.class,
                        () -> new SnmpSinkRowConverter(config(), missingField));
        Assertions.assertTrue(missing.getMessage().contains("value_type"));

        SeaTunnelRowType wrongType =
                new SeaTunnelRowType(
                        new String[] {"oid", "value", "value_type"},
                        new SeaTunnelDataType[] {
                            BasicType.STRING_TYPE, BasicType.INT_TYPE, BasicType.STRING_TYPE
                        });
        SnmpConnectorException invalidType =
                Assertions.assertThrows(
                        SnmpConnectorException.class,
                        () -> new SnmpSinkRowConverter(config(), wrongType));
        Assertions.assertTrue(invalidType.getMessage().contains("must use STRING"));
    }

    @Test
    void testRejectsInvalidRowsBeforeNetworkIo() {
        SnmpSinkRowConverter converter = new SnmpSinkRowConverter(config(), sinkRowType());

        Assertions.assertThrows(
                SnmpConnectorException.class,
                () -> converter.convert(row("not-an-oid", "1", "Integer32")));
        Assertions.assertThrows(
                SnmpConnectorException.class,
                () -> converter.convert(row("1.3.6.1.2.1.1.5.0", "1", "UnknownType")));
        Assertions.assertThrows(
                SnmpConnectorException.class,
                () -> converter.convert(row("1.3.6.1.2.1.1.5.0", "4294967296", "Counter32")));
        Assertions.assertThrows(
                SnmpConnectorException.class,
                () -> converter.convert(row("1.3.6.1.2.1.1.5.0", "0fg1", "OctetStringHex")));
        Assertions.assertThrows(
                SnmpConnectorException.class,
                () -> converter.convert(row("1.3.6.1.2.1.1.5.0", "999.0.2.1", "IpAddress")));
        Assertions.assertThrows(
                SnmpConnectorException.class,
                () -> converter.convert(row("1.3.6.1.2.1.1.5.0", null, "OctetString")));
        Assertions.assertThrows(
                SnmpConnectorException.class,
                () ->
                        converter.convert(
                                new SeaTunnelRow(
                                        new Object[] {"1.3.6.1.2.1.1.5.0", 1, "Integer32"})));
        Assertions.assertThrows(
                SnmpConnectorException.class,
                () ->
                        converter.convert(
                                row("1.3.6.1.2.1.1.5.0", "18446744073709551616", "Counter64")));
        Assertions.assertThrows(
                SnmpConnectorException.class,
                () -> converter.convert(row("1.3.6.1.2.1.1.5.0", "0:60:00.00", "TimeTicks")));
        Assertions.assertThrows(
                SnmpConnectorException.class,
                () ->
                        converter.convert(
                                row("1.3.6.1.2.1.1.5.0", "2 day, 0:00:00.00", "TimeTicks")));
        Assertions.assertThrows(
                SnmpConnectorException.class,
                () ->
                        converter.convert(
                                row("1.3.6.1.2.1.1.5.0", "0 days, 0:00:00.00", "TimeTicks")));
        Assertions.assertThrows(
                SnmpConnectorException.class,
                () ->
                        converter.convert(
                                row("1.3.6.1.2.1.1.5.0", "498 days, 0:00:00.00", "TimeTicks")));
    }

    private static void assertVariable(
            String type,
            String value,
            Class<? extends Variable> expectedClass,
            String expectedText) {
        Variable variable = SnmpSinkRowConverter.parseVariable(type, value);
        Assertions.assertInstanceOf(expectedClass, variable);
        Assertions.assertEquals(expectedText, variable.toString());
    }

    private static void assertSourceVariable(
            Variable sourceVariable, Class<? extends Variable> expectedClass) {
        Variable converted =
                SnmpSinkRowConverter.parseVariable(
                        sourceVariable.getSyntaxString(), sourceVariable.toString());
        Assertions.assertInstanceOf(expectedClass, converted);
        Assertions.assertEquals(sourceVariable.toString(), converted.toString());
    }

    private static SeaTunnelRow row(String oid, String value, String valueType) {
        return new SeaTunnelRow(new Object[] {oid, value, valueType});
    }

    static SeaTunnelRowType sinkRowType() {
        return new SeaTunnelRowType(
                new String[] {"oid", "value", "value_type"},
                new SeaTunnelDataType[] {
                    BasicType.STRING_TYPE, BasicType.STRING_TYPE, BasicType.STRING_TYPE
                });
    }

    private static SeaTunnelRowType sourceRowType() {
        return new SeaTunnelRowType(
                new String[] {"agent", "oid", "value", "value_type", "poll_time"},
                new SeaTunnelDataType[] {
                    BasicType.STRING_TYPE,
                    BasicType.STRING_TYPE,
                    BasicType.STRING_TYPE,
                    BasicType.STRING_TYPE,
                    BasicType.LONG_TYPE
                });
    }

    static SnmpSinkConfig config() {
        Map<String, Object> values = new HashMap<>();
        values.put("host", "127.0.0.1");
        values.put("community", "unit-test-community");
        return new SnmpSinkConfig(ReadonlyConfig.fromMap(values));
    }
}
