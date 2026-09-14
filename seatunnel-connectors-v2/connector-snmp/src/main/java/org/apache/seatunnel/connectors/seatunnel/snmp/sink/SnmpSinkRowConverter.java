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

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.connectors.seatunnel.snmp.config.SnmpSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.snmp.exception.SnmpConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.snmp.exception.SnmpConnectorException;

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

import java.io.Serializable;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Converts the configured input fields into one validated SNMP SET binding. */
final class SnmpSinkRowConverter implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final long MAX_UNSIGNED_32 = 0xFFFFFFFFL;
    private static final BigInteger MAX_UNSIGNED_64 = new BigInteger("18446744073709551615");
    private static final Pattern NUMERIC_OID = Pattern.compile("^\\.?[0-9]+(\\.[0-9]+)+$");
    private static final Pattern HEX_VALUE = Pattern.compile("^[0-9a-fA-F]*$");
    private static final Pattern IPV4_VALUE = Pattern.compile("^(?:[0-9]{1,3}\\.){3}[0-9]{1,3}$");
    private static final Pattern DECIMAL_INTEGER = Pattern.compile("^[+-]?[0-9]+$");
    private static final Pattern FORMATTED_TIME_TICKS =
            Pattern.compile(
                    "^(?:([0-9]+) (day|days), )?([0-9]{1,2}):([0-9]{2}):([0-9]{2})\\.([0-9]{2})$");
    private static final BigInteger TICKS_PER_DAY = BigInteger.valueOf(8_640_000L);
    private static final BigInteger TICKS_PER_HOUR = BigInteger.valueOf(360_000L);
    private static final BigInteger TICKS_PER_MINUTE = BigInteger.valueOf(6_000L);
    private static final BigInteger TICKS_PER_SECOND = BigInteger.valueOf(100L);

    private final int rowArity;
    private final int oidIndex;
    private final int valueIndex;
    private final int valueTypeIndex;

    SnmpSinkRowConverter(SnmpSinkConfig config, SeaTunnelRowType rowType) {
        this.rowArity = rowType.getTotalFields();
        this.oidIndex = requireStringField(rowType, config.getOidField(), "oid_field");
        this.valueIndex = requireStringField(rowType, config.getValueField(), "value_field");
        this.valueTypeIndex =
                requireStringField(rowType, config.getValueTypeField(), "value_type_field");
    }

    SnmpSetRequest convert(SeaTunnelRow row) {
        if (row.getArity() != rowArity) {
            throw invalidRow(
                    "Input row arity "
                            + row.getArity()
                            + " does not match the configured schema arity "
                            + rowArity);
        }

        String oid = requireNonBlankRowValue(row, oidIndex, "OID");
        String value = requireNonNullRowValue(row, valueIndex, "value");
        String valueType = requireNonBlankRowValue(row, valueTypeIndex, "value type");
        return new SnmpSetRequest(parseOid(oid), parseVariable(valueType, value));
    }

    private static int requireStringField(
            SeaTunnelRowType rowType, String fieldName, String optionName) {
        int index = rowType.indexOf(fieldName, false);
        if (index < 0) {
            throw invalidConfig(
                    "Option `"
                            + optionName
                            + "` references unknown field `"
                            + fieldName
                            + "`. Available fields are "
                            + Arrays.toString(rowType.getFieldNames()));
        }
        SeaTunnelDataType<?> dataType = rowType.getFieldType(index);
        if (dataType.getSqlType() != SqlType.STRING) {
            throw invalidConfig(
                    "Field `"
                            + fieldName
                            + "` configured by `"
                            + optionName
                            + "` must use STRING type, but was "
                            + dataType.getSqlType());
        }
        return index;
    }

    private static String requireNonBlankRowValue(
            SeaTunnelRow row, int index, String fieldDescription) {
        String stringValue = requireNonNullRowValue(row, index, fieldDescription);
        if (stringValue.trim().isEmpty()) {
            throw invalidRow("SNMP sink " + fieldDescription + " field must not be blank");
        }
        return stringValue.trim();
    }

    private static String requireNonNullRowValue(
            SeaTunnelRow row, int index, String fieldDescription) {
        Object value = row.getField(index);
        if (value == null) {
            throw invalidRow("SNMP sink " + fieldDescription + " field must not be null");
        }
        if (!(value instanceof String)) {
            throw invalidRow("SNMP sink " + fieldDescription + " field must contain a STRING");
        }
        return (String) value;
    }

    static OID parseOid(String configuredOid) {
        String value = configuredOid.trim();
        if (!NUMERIC_OID.matcher(value).matches()) {
            throw invalidRow("SNMP sink OID must be numeric: " + configuredOid);
        }
        if (value.charAt(0) == '.') {
            value = value.substring(1);
        }
        try {
            OID oid = new OID(value);
            if (!oid.isValid()) {
                throw invalidRow("SNMP sink OID is invalid: " + configuredOid);
            }
            return oid;
        } catch (SnmpConnectorException e) {
            throw e;
        } catch (RuntimeException e) {
            throw invalidRow("SNMP sink OID is invalid: " + configuredOid, e);
        }
    }

    static Variable parseVariable(String configuredType, String value) {
        String normalizedType = normalizeType(configuredType);
        try {
            switch (normalizedType) {
                case "INTEGER":
                case "INTEGER32":
                    return new Integer32(Integer.parseInt(value));
                case "UNSIGNEDINTEGER":
                case "UNSIGNEDINTEGER32":
                    return new UnsignedInteger32(parseUnsigned32(value, configuredType));
                case "COUNTER":
                case "COUNTER32":
                    return new Counter32(parseUnsigned32(value, configuredType));
                case "GAUGE":
                case "GAUGE32":
                    return new Gauge32(parseUnsigned32(value, configuredType));
                case "TIMETICKS":
                    return new TimeTicks(parseTimeTicks(value, configuredType));
                case "COUNTER64":
                    return new Counter64(parseUnsigned64(value, configuredType));
                case "OCTETSTRING":
                    return new OctetString(value.getBytes(StandardCharsets.UTF_8));
                case "OCTETSTRINGHEX":
                    return new OctetString(parseHex(value));
                case "OBJECTIDENTIFIER":
                case "OID":
                    return parseOid(value);
                case "IPADDRESS":
                    validateIpv4(value);
                    return new IpAddress(value);
                default:
                    throw invalidRow(
                            "Unsupported SNMP sink value type `"
                                    + configuredType
                                    + "`. Supported types are Integer32, UnsignedInteger32, "
                                    + "Counter/Counter32, Gauge/Gauge32, TimeTicks, Counter64, "
                                    + "OctetString/OCTET STRING, OctetStringHex, "
                                    + "OID/OBJECT IDENTIFIER, and IpAddress");
            }
        } catch (SnmpConnectorException e) {
            throw e;
        } catch (RuntimeException e) {
            throw invalidRow("SNMP sink value is invalid for type `" + configuredType + "`", e);
        }
    }

    private static String normalizeType(String configuredType) {
        StringBuilder normalized = new StringBuilder(configuredType.length());
        for (int index = 0; index < configuredType.length(); index++) {
            char character = configuredType.charAt(index);
            if (character != '_' && character != '-' && !Character.isWhitespace(character)) {
                normalized.append(character);
            }
        }
        return normalized.toString().toUpperCase(Locale.ROOT);
    }

    private static long parseUnsigned32(String value, String configuredType) {
        long parsed = Long.parseLong(value);
        if (parsed < 0 || parsed > MAX_UNSIGNED_32) {
            throw invalidRow(
                    "SNMP sink value for type `"
                            + configuredType
                            + "` must be between 0 and "
                            + MAX_UNSIGNED_32);
        }
        return parsed;
    }

    private static long parseTimeTicks(String value, String configuredType) {
        if (DECIMAL_INTEGER.matcher(value).matches()) {
            return parseUnsigned32(value, configuredType);
        }

        Matcher matcher = FORMATTED_TIME_TICKS.matcher(value);
        if (!matcher.matches()) {
            throw invalidRow(
                    "SNMP sink TimeTicks value must be an unsigned decimal count or use the "
                            + "SNMP4J format `[days, ]hours:mm:ss.hh`");
        }

        BigInteger days =
                matcher.group(1) == null ? BigInteger.ZERO : new BigInteger(matcher.group(1));
        String dayUnit = matcher.group(2);
        if ((matcher.group(1) != null && BigInteger.ZERO.equals(days))
                || (BigInteger.ONE.equals(days) && !"day".equals(dayUnit))
                || (!BigInteger.ONE.equals(days) && "day".equals(dayUnit))) {
            throw invalidRow("SNMP sink TimeTicks day unit does not match its value");
        }

        int hours = Integer.parseInt(matcher.group(3));
        int minutes = Integer.parseInt(matcher.group(4));
        int seconds = Integer.parseInt(matcher.group(5));
        int hundredths = Integer.parseInt(matcher.group(6));
        if (hours > 23 || minutes > 59 || seconds > 59) {
            throw invalidRow("SNMP sink TimeTicks formatted value is outside clock bounds");
        }

        BigInteger ticks =
                days.multiply(TICKS_PER_DAY)
                        .add(BigInteger.valueOf(hours).multiply(TICKS_PER_HOUR))
                        .add(BigInteger.valueOf(minutes).multiply(TICKS_PER_MINUTE))
                        .add(BigInteger.valueOf(seconds).multiply(TICKS_PER_SECOND))
                        .add(BigInteger.valueOf(hundredths));
        if (ticks.compareTo(BigInteger.valueOf(MAX_UNSIGNED_32)) > 0) {
            throw invalidRow("SNMP sink TimeTicks value must be between 0 and " + MAX_UNSIGNED_32);
        }
        return ticks.longValue();
    }

    private static long parseUnsigned64(String value, String configuredType) {
        BigInteger parsed = new BigInteger(value);
        if (parsed.signum() < 0 || parsed.compareTo(MAX_UNSIGNED_64) > 0) {
            throw invalidRow(
                    "SNMP sink value for type `"
                            + configuredType
                            + "` must be between 0 and "
                            + MAX_UNSIGNED_64);
        }
        return parsed.longValue();
    }

    private static byte[] parseHex(String value) {
        if ((value.length() & 1) != 0 || !HEX_VALUE.matcher(value).matches()) {
            throw invalidRow(
                    "SNMP sink OctetStringHex value must contain an even number of hexadecimal characters");
        }
        byte[] bytes = new byte[value.length() / 2];
        for (int index = 0; index < value.length(); index += 2) {
            bytes[index / 2] = (byte) Integer.parseInt(value.substring(index, index + 2), 16);
        }
        return bytes;
    }

    private static void validateIpv4(String value) {
        if (!IPV4_VALUE.matcher(value).matches()) {
            throw invalidRow("SNMP sink IpAddress value must be a dotted IPv4 address");
        }
        for (String octet : value.split("\\.")) {
            if (Integer.parseInt(octet) > 255) {
                throw invalidRow("SNMP sink IpAddress value must be a dotted IPv4 address");
            }
        }
    }

    private static SnmpConnectorException invalidConfig(String message) {
        return new SnmpConnectorException(SnmpConnectorErrorCode.INVALID_CONFIG, message);
    }

    private static SnmpConnectorException invalidRow(String message) {
        return new SnmpConnectorException(SnmpConnectorErrorCode.INVALID_ROW, message);
    }

    private static SnmpConnectorException invalidRow(String message, Throwable cause) {
        return new SnmpConnectorException(SnmpConnectorErrorCode.INVALID_ROW, message, cause);
    }
}
