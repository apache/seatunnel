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

package io.debezium.connector.oracle.logminer.parser;

import io.debezium.DebeziumException;
import io.debezium.connector.oracle.logminer.events.LogMinerEventRow;
import io.debezium.util.Strings;
import oracle.sql.CHAR;
import oracle.sql.CharacterSet;
import oracle.sql.RAW;

import java.sql.SQLException;

/**
 * Parser for decoding an {@code XML_WRITE} LogMiner REDO_SQL value.
 *
 * <p>Official (Debezium 3.7.1.Final): {@code
 * debezium-connector-oracle/.../parser/XmlWriteParser.java}. Branching is the same: {@code XML_REDO
 * := NULL} / blank SQL is null; {@code INFO} ending with {@code not re-executable} uses {@code
 * XML_REDO} / {@code HEXTORAW}; otherwise {@code SQL_REDO} is the document text.
 *
 * <p>SeaTunnel changes:
 *
 * <ul>
 *   <li>Java 8 nested {@link Payload} instead of {@code record XmlWrite}
 *   <li>{@code [SeaTunnel]} HEXTORAW stays as bytes here and is decoded once in {@link
 *       io.debezium.connector.oracle.logminer.processor.TransactionCommitConsumer} so a character
 *       split across chunks is not corrupted. Official 3.7.1.Final does {@code new CHAR(bytes,
 *       charset).toString()} per chunk. See <a
 *       href="https://github.com/debezium/dbz/issues/1373">debezium/dbz#1373</a> and <a
 *       href="https://github.com/debezium/dbz/issues/1798">debezium/dbz#1798</a>
 * </ul>
 *
 * @author Chris Cranford
 */
public final class XmlWriteParser {

    static final String XML_WRITE_PREAMBLE = "XML_REDO := ";
    private static final String XML_WRITE_PREAMBLE_NULL = XML_WRITE_PREAMBLE + "NULL";

    private XmlWriteParser() {}

    public static Payload parse(LogMinerEventRow event) throws SQLException {
        return parse(event.getRedoSql(), event.getInfo());
    }

    public static Payload parse(String redoSql, String info) throws SQLException {
        if (XML_WRITE_PREAMBLE_NULL.equals(redoSql) || Strings.isNullOrBlank(redoSql)) {
            return Payload.nullValue();
        }
        if (XmlParserUtils.isXmlSerializedAsBinary(info)) {
            return parseBinary(redoSql);
        }
        return Payload.text(redoSql, redoSql.length());
    }

    private static Payload parseBinary(String redoSql) throws SQLException {
        if (!redoSql.startsWith(XML_WRITE_PREAMBLE)) {
            throw new IllegalStateException(
                    "XML write operation does not start with XML_REDO preamble");
        }
        if (redoSql.charAt(XML_WRITE_PREAMBLE.length()) == '\'') {
            int lastQuoteIndex = redoSql.lastIndexOf('\'');
            if (lastQuoteIndex == -1) {
                throw new IllegalStateException("Failed to find end of XML document");
            }
            String xml = redoSql.substring(XML_WRITE_PREAMBLE.length() + 1, lastQuoteIndex);
            return Payload.text(xml, lengthOf(redoSql));
        }

        int lastParenIndex = redoSql.lastIndexOf(')');
        if (lastParenIndex == -1) {
            throw new IllegalStateException("Failed to find end of XML document");
        }
        String xmlHex = redoSql.substring(XML_WRITE_PREAMBLE.length(), lastParenIndex + 1);
        if (!xmlHex.startsWith("HEXTORAW('") || !xmlHex.endsWith(")")) {
            throw new IllegalStateException("Invalid HEXTORAW XML decoded data");
        }
        if (xmlHex.endsWith("')")) {
            xmlHex = xmlHex.substring(10, xmlHex.length() - 2);
        } else {
            // Oracle sometimes omits the closing quote before the final parenthesis.
            xmlHex = xmlHex.substring(10, xmlHex.length() - 1);
        }
        return Payload.bytes(RAW.hexString2Bytes(xmlHex), lengthOf(redoSql));
    }

    /**
     * Decodes assembled {@code HEXTORAW} bytes with the database character set. Debezium does this
     * per chunk; assembling first keeps a character that LogMiner split across writes. See <a
     * href="https://github.com/debezium/dbz/issues/1798">debezium/dbz#1798</a>.
     */
    public static String decodeHexBytes(byte[] bytes, CharacterSet characterSet) {
        if (bytes == null || bytes.length == 0) {
            return "";
        }
        CharacterSet cs =
                characterSet != null
                        ? characterSet
                        : CharacterSet.make(CharacterSet.AL32UTF8_CHARSET);
        try {
            return new CHAR(bytes, 0, bytes.length, cs).toString();
        } catch (SQLException e) {
            throw new DebeziumException("Failed to decode XML HEXTORAW bytes", e);
        }
    }

    private static int lengthOf(String sql) {
        int lastColonIndex = sql.lastIndexOf(':');
        if (lastColonIndex == -1) {
            throw new IllegalStateException("Failed to find XML document length");
        }
        return Integer.parseInt(sql.substring(lastColonIndex + 1).trim());
    }

    public static final class Payload {
        private final String text;
        private final byte[] bytes;
        private final int length;
        private final boolean nullValue;

        private Payload(String text, byte[] bytes, int length, boolean nullValue) {
            this.text = text;
            this.bytes = bytes;
            this.length = length;
            this.nullValue = nullValue;
        }

        static Payload nullValue() {
            return new Payload(null, null, 0, true);
        }

        static Payload text(String text, int length) {
            return new Payload(text, null, length, false);
        }

        static Payload bytes(byte[] bytes, int length) {
            return new Payload(null, bytes, length, false);
        }

        public String getText() {
            return text;
        }

        public byte[] getBytes() {
            return bytes;
        }

        public int getLength() {
            return length;
        }

        public boolean isNullValue() {
            return nullValue;
        }
    }
}
