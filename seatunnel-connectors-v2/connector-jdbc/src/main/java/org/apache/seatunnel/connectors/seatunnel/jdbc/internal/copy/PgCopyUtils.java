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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.copy;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.UUID;

/** Common utility functions for COPY readers. */
public final class PgCopyUtils {

    private static final Logger LOG = LoggerFactory.getLogger(PgCopyUtils.class);

    private PgCopyUtils() {}

    public static void closeQuietly(Object obj) {
        if (obj instanceof Closeable) {
            Closeable c = (Closeable) obj;
            try {
                c.close();
            } catch (Exception e) {
                LOG.warn("Failed to close resource: {}", obj.getClass().getSimpleName(), e);
            }
        }
    }

    public static Object parseBinaryField(
            byte[] bytes,
            SeaTunnelDataType<?> type,
            LocalDate epochDate,
            LocalDateTime epochDateTime) {
        return parseBinaryField(bytes, type, null, epochDate, epochDateTime);
    }

    public static Object parseBinaryField(
            byte[] bytes,
            SeaTunnelDataType<?> type,
            String sourceType,
            LocalDate epochDate,
            LocalDateTime epochDateTime) {

        if (bytes == null) {
            return null;
        }

        ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);

        try {
            switch (type.getSqlType()) {
                case STRING:
                    if (sourceType != null) {
                        if ("uuid".equalsIgnoreCase(sourceType)) {
                            long msb = buf.getLong();
                            long lsb = buf.getLong();
                            return new UUID(msb, lsb).toString();
                        }
                        if ("geometry".equalsIgnoreCase(sourceType)
                                || "geography".equalsIgnoreCase(sourceType)) {
                            return Hex.encodeHexString(bytes);
                        }
                        if ("jsonb".equalsIgnoreCase(sourceType)) {
                            if (bytes.length > 0 && (bytes[0] == 0x01)) {
                                return new String(
                                        bytes,
                                        1,
                                        bytes.length - 1,
                                        java.nio.charset.StandardCharsets.UTF_8);
                            }
                            return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
                        }
                    }
                    return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
                case BOOLEAN:
                    return buf.get() != 0;
                case TINYINT:
                    return buf.get();
                case SMALLINT:
                    return buf.getShort();
                case INT:
                    return buf.getInt();
                case BIGINT:
                    return buf.getLong();
                case FLOAT:
                    return buf.getFloat();
                case DOUBLE:
                    return buf.getDouble();
                case DECIMAL:
                    return PgNumericDecoder.decode(buf);
                case DATE:
                    return epochDate.plusDays(buf.getInt());
                case TIME:
                    return java.time.LocalTime.ofNanoOfDay(buf.getLong() * 1000L);
                case TIMESTAMP:
                    return epochDateTime.plusNanos(buf.getLong() * 1000L);
                case TIMESTAMP_TZ:
                    return epochDateTime.atOffset(ZoneOffset.UTC).plusNanos(buf.getLong() * 1000L);
                case BYTES:
                    return bytes;
                default:
                    throw new JdbcConnectorException(
                            CommonErrorCodeDeprecated.UNSUPPORTED_DATA_TYPE,
                            "Unsupported binary type: " + type);
            }
        } catch (Exception e) {
            throw new JdbcConnectorException(
                    CommonErrorCodeDeprecated.UNSUPPORTED_DATA_TYPE,
                    "Failed to parse binary field for type: " + type,
                    e);
        }
    }

    public static Object parseBinaryField(
            ByteBuffer buf,
            SeaTunnelDataType<?> type,
            LocalDate epochDate,
            LocalDateTime epochDateTime) {
        return parseBinaryField(buf, type, null, epochDate, epochDateTime);
    }

    public static Object parseBinaryField(
            ByteBuffer buf,
            SeaTunnelDataType<?> type,
            String sourceType,
            LocalDate epochDate,
            LocalDateTime epochDateTime) {
        try {
            switch (type.getSqlType()) {
                case STRING:
                    if (sourceType != null) {
                        if ("uuid".equalsIgnoreCase(sourceType)) {
                            long msb = buf.getLong();
                            long lsb = buf.getLong();
                            return new UUID(msb, lsb).toString();
                        }
                        if ("geometry".equalsIgnoreCase(sourceType)
                                || "geography".equalsIgnoreCase(sourceType)) {
                            byte[] g = new byte[buf.remaining()];
                            buf.get(g);
                            return Hex.encodeHexString(g);
                        }
                        if ("jsonb".equalsIgnoreCase(sourceType)) {
                            if (buf.remaining() == 0) {
                                return null;
                            }
                            int pos = buf.position();
                            int len = buf.remaining();
                            if (buf.get(pos) == 0x01 && len > 1) {
                                byte[] b = new byte[len - 1];
                                buf.position(pos + 1);
                                buf.get(b);
                                return new String(b, java.nio.charset.StandardCharsets.UTF_8);
                            } else {
                                byte[] b = new byte[len];
                                buf.get(b);
                                return new String(b, java.nio.charset.StandardCharsets.UTF_8);
                            }
                        }
                    }
                    if (buf.hasArray()) {
                        return new String(
                                buf.array(),
                                buf.position(),
                                buf.remaining(),
                                java.nio.charset.StandardCharsets.UTF_8);
                    } else {
                        return java.nio.charset.StandardCharsets.UTF_8
                                .decode(buf.slice())
                                .toString();
                    }
                case BOOLEAN:
                    return buf.get() != 0;
                case TINYINT:
                    return buf.get();
                case SMALLINT:
                    return buf.getShort();
                case INT:
                    return buf.getInt();
                case BIGINT:
                    return buf.getLong();
                case FLOAT:
                    return buf.getFloat();
                case DOUBLE:
                    return buf.getDouble();
                case DECIMAL:
                    return PgNumericDecoder.decode(buf.slice());
                case DATE:
                    return epochDate.plusDays(buf.getInt());
                case TIME:
                    return java.time.LocalTime.ofNanoOfDay(buf.getLong() * 1000L);
                case TIMESTAMP:
                    return epochDateTime.plusNanos(buf.getLong() * 1000L);
                case TIMESTAMP_TZ:
                    return epochDateTime.atOffset(ZoneOffset.UTC).plusNanos(buf.getLong() * 1000L);
                case BYTES:
                    {
                        byte[] out = new byte[buf.remaining()];
                        buf.get(out);
                        return out;
                    }
                default:
                    throw new JdbcConnectorException(
                            CommonErrorCodeDeprecated.UNSUPPORTED_DATA_TYPE,
                            "Unsupported binary type: " + type);
            }
        } catch (Exception e) {
            throw new JdbcConnectorException(
                    CommonErrorCodeDeprecated.UNSUPPORTED_DATA_TYPE,
                    "Failed to parse binary field for type: " + type,
                    e);
        }
    }

    public static Object parseValue(String raw, SeaTunnelDataType<?> type) {
        if (raw == null || raw.isEmpty() || "\\N".equals(raw)) {
            return null;
        }

        try {
            switch (type.getSqlType()) {
                case STRING:
                    return raw;
                case BOOLEAN:
                    if ("t".equalsIgnoreCase(raw)
                            || "true".equalsIgnoreCase(raw)
                            || "y".equalsIgnoreCase(raw)
                            || "yes".equalsIgnoreCase(raw)
                            || "1".equals(raw)) {
                        return true;
                    }
                    if ("f".equalsIgnoreCase(raw)
                            || "false".equalsIgnoreCase(raw)
                            || "n".equalsIgnoreCase(raw)
                            || "no".equalsIgnoreCase(raw)
                            || "0".equals(raw)) {
                        return false;
                    }
                    return Boolean.valueOf(raw);
                case TINYINT:
                    return Byte.valueOf(raw);
                case SMALLINT:
                    return Short.valueOf(raw);
                case INT:
                    return Integer.valueOf(raw);
                case BIGINT:
                    return Long.valueOf(raw);
                case FLOAT:
                    return Float.valueOf(raw);
                case DOUBLE:
                    return Double.valueOf(raw);
                case DECIMAL:
                    return new BigDecimal(raw);
                case DATE:
                    return Date.valueOf(raw).toLocalDate();
                case TIME:
                    return Time.valueOf(raw).toLocalTime();
                case TIMESTAMP:
                    return Timestamp.valueOf(raw).toLocalDateTime();
                case BYTES:
                    return Base64.decodeBase64(raw);
                default:
                    throw new JdbcConnectorException(
                            CommonErrorCodeDeprecated.UNSUPPORTED_DATA_TYPE,
                            "Unsupported CSV type: " + type);
            }
        } catch (Exception e) {
            throw new JdbcConnectorException(
                    CommonErrorCodeDeprecated.UNSUPPORTED_DATA_TYPE,
                    "Failed to parse CSV field for type: " + type,
                    e);
        }
    }
}
