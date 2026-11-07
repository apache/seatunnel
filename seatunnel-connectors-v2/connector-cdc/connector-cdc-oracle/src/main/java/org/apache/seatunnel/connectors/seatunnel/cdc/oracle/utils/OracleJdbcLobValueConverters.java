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

package org.apache.seatunnel.connectors.seatunnel.cdc.oracle.utils;

import org.apache.kafka.connect.data.Field;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig.BinaryHandlingMode;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleValueConverters;
import io.debezium.relational.Column;

import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;

/**
 * Oracle value converters that keep real JDBC LOB values during snapshot.
 *
 * <p>Debezium 1.9.8 {@link OracleValueConverters} returns null or an empty value for {@link Clob}
 * and {@link Blob} objects when {@code lob.enabled} is false. Snapshot reads those objects with a
 * plain JDBC {@code SELECT}, so the initial snapshot would otherwise drop CLOB, NCLOB, and BLOB
 * data. Streaming does not pass JDBC locators for unavailable LOBs; it passes {@link
 * OracleValueConverters#UNAVAILABLE_VALUE}, which still becomes the configured placeholder.
 */
public class OracleJdbcLobValueConverters extends OracleValueConverters {

    private final boolean lobEnabled;

    public OracleJdbcLobValueConverters(OracleConnectorConfig config, OracleConnection connection) {
        super(config, connection);
        this.lobEnabled = config.isLobEnabled();
    }

    /**
     * Materializes a JDBC {@link Clob} or {@link java.sql.NClob} regardless of {@code lob.enabled}.
     * Every other value, including the streaming unavailable marker, keeps Debezium's conversion.
     */
    @Override
    protected Object convertString(Column column, Field fieldDefn, Object data) {
        if (data instanceof Clob) {
            return readClob(column, (Clob) data);
        }
        // Streaming redo uses the function text. With LOB mining on, that is a real empty LOB,
        // the same value a JDBC snapshot reads from an empty locator. SQL NULL is not this text.
        if (lobEnabled && OracleValueConverters.EMPTY_CLOB_FUNCTION.equals(data)) {
            return "";
        }
        return super.convertString(column, fieldDefn, data);
    }

    /**
     * Materializes a JDBC {@link Blob} regardless of {@code lob.enabled}. The bytes are then passed
     * through Debezium's binary handling mode. Streaming placeholders are unchanged.
     */
    @Override
    protected Object convertBinary(
            Column column, Field fieldDefn, Object data, BinaryHandlingMode mode) {
        if (lobEnabled && OracleValueConverters.EMPTY_BLOB_FUNCTION.equals(data)) {
            data = new byte[0];
        } else if (data instanceof Blob) {
            data = readBlob(column, (Blob) data);
        }
        return super.convertBinary(column, fieldDefn, data, mode);
    }

    private static String readClob(Column column, Clob clob) {
        try {
            long length = clob.length();
            if (length > Integer.MAX_VALUE) {
                throw new DebeziumException(
                        "CLOB value for column "
                                + column.name()
                                + " is larger than the supported 2GB limit");
            }
            // java.sql.Clob is 1-based. getSubString(1, 0) is valid for an empty LOB.
            return clob.getSubString(1, (int) length);
        } catch (SQLException e) {
            throw new DebeziumException(
                    "Couldn't convert CLOB value for column " + column.name(), e);
        }
    }

    private static byte[] readBlob(Column column, Blob blob) {
        try {
            long length = blob.length();
            if (length > Integer.MAX_VALUE) {
                throw new DebeziumException(
                        "BLOB value for column "
                                + column.name()
                                + " is larger than the supported 2GB limit");
            }
            return blob.getBytes(1, (int) length);
        } catch (SQLException e) {
            throw new DebeziumException(
                    "Couldn't convert BLOB value for column " + column.name(), e);
        }
    }
}
