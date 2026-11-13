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

package io.debezium.connector.oracle.logminer.processor;

import oracle.sql.RAW;

import java.sql.SQLException;

/**
 * Parses one LogMiner {@code XML_WRITE} redo statement.
 *
 * <p>Out-of-line documents arrive as {@code HEXTORAW} chunks. Those chunks are returned as raw
 * bytes. A multi-byte UTF-8 character can be split across chunks, so the bytes are decoded once
 * when the document is assembled. Inline documents arrive as a quoted SQL string and are already
 * characters. Oracle 11g and 21c also emit the document text directly, without the {@code XML_REDO}
 * preamble.
 */
final class XmlWriteParser {

    static final String XML_WRITE_PREAMBLE = "XML_REDO := ";
    private static final String XML_WRITE_PREAMBLE_NULL = XML_WRITE_PREAMBLE + "NULL";

    private XmlWriteParser() {}

    static Payload parse(String sql) throws SQLException {
        if (sql == null) {
            throw new IllegalStateException("XML write operation does not contain document text");
        }
        String body = sql.trim();
        if (!body.startsWith(XML_WRITE_PREAMBLE)) {
            // 11g and 21c LogMiner put the document text itself in SQL_REDO.
            return Payload.text(sql, sql.codePointCount(0, sql.length()));
        }
        if (XML_WRITE_PREAMBLE_NULL.equals(body)) {
            return Payload.nullValue();
        }
        if (body.charAt(XML_WRITE_PREAMBLE.length()) == '\'') {
            int lastQuoteIndex = body.lastIndexOf('\'');
            if (lastQuoteIndex == -1) {
                throw new IllegalStateException("Failed to find end of XML document");
            }
            String xml =
                    LogMinerSqlParser.unescapeLiteral(
                            body.substring(XML_WRITE_PREAMBLE.length() + 1, lastQuoteIndex));
            return Payload.text(xml, lengthOf(body));
        }

        int lastParenIndex = body.lastIndexOf(')');
        if (lastParenIndex == -1) {
            throw new IllegalStateException("Failed to find end of XML document");
        }
        String xmlHex = body.substring(XML_WRITE_PREAMBLE.length(), lastParenIndex + 1);
        if (!xmlHex.startsWith("HEXTORAW('") || !xmlHex.endsWith(")")) {
            throw new IllegalStateException("Invalid HEXTORAW XML decoded data");
        }
        if (xmlHex.endsWith("')")) {
            xmlHex = xmlHex.substring(10, xmlHex.length() - 2);
        } else {
            // Oracle sometimes omits the closing quote before the final parenthesis.
            xmlHex = xmlHex.substring(10, xmlHex.length() - 1);
        }
        return Payload.bytes(RAW.hexString2Bytes(xmlHex), lengthOf(body));
    }

    private static int lengthOf(String sql) {
        int lastColonIndex = sql.lastIndexOf(':');
        if (lastColonIndex == -1) {
            throw new IllegalStateException("Failed to find XML document length");
        }
        return Integer.parseInt(sql.substring(lastColonIndex + 1).trim());
    }

    static final class Payload {
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

        String getText() {
            return text;
        }

        byte[] getBytes() {
            return bytes;
        }

        int getLength() {
            return length;
        }

        boolean isNullValue() {
            return nullValue;
        }
    }
}
