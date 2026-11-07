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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.events.EventType;
import io.debezium.connector.oracle.logminer.events.XmlWriteEvent;
import io.debezium.connector.oracle.logminer.processor.TransactionCommitConsumer.XmlFragment;
import io.debezium.connector.oracle.logminer.processor.TransactionCommitConsumer.XmlUnderConstruction;
import io.debezium.relational.TableId;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;

public class XmlWriteAssemblyTest {

    private static final TableId TABLE = TableId.parse("DEBEZIUM.XML_TEST");

    @Test
    public void decodesMultiByteCharactersSplitAcrossHexChunks() throws Exception {
        assertHexSplit("é", 1);
        assertHexSplit("中", 1);
        assertHexSplit("中", 2);
        assertHexSplit("\uD83D\uDE00", 1);
        assertHexSplit("\uD83D\uDE00", 2);
        assertHexSplit("\uD83D\uDE00", 3);

        byte[] document = "数符癸".getBytes(StandardCharsets.UTF_8);
        int split = "数".getBytes(StandardCharsets.UTF_8).length + 1;
        assertHexSplit(
                "数符癸",
                Arrays.copyOfRange(document, 0, split),
                Arrays.copyOfRange(document, split, document.length));
    }

    @Test
    public void keepsASurrogatePairSplitAcrossQuotedChunks() {
        XmlUnderConstruction xml = new XmlUnderConstruction();
        xml.add(new XmlFragment("\uD83D"));
        xml.add(new XmlFragment("\uDE00"));

        Assertions.assertEquals("\uD83D\uDE00", xml.merge());
    }

    @Test
    public void parsesInlineXmlAsTextAndNullAsNull() throws Exception {
        XmlWriteParser.Payload text = XmlWriteParser.parse("XML_REDO := '<a>数符癸</a>':10");
        Assertions.assertEquals("<a>数符癸</a>", text.getText());
        Assertions.assertNull(text.getBytes());
        Assertions.assertEquals(10, text.getLength());

        XmlWriteParser.Payload nullXml = XmlWriteParser.parse("XML_REDO := NULL");
        Assertions.assertTrue(nullXml.isNullValue());
        Assertions.assertNull(nullXml.getText());
        Assertions.assertNull(nullXml.getBytes());
    }

    @Test
    public void parsesHexChunksAsBytesIncludingTheUnclosedQuoteForm() throws Exception {
        byte[] bytes = "é".getBytes(StandardCharsets.UTF_8);
        XmlWriteParser.Payload quoted = XmlWriteParser.parse(hexSql(bytes, true));
        Assertions.assertArrayEquals(bytes, quoted.getBytes());
        Assertions.assertNull(quoted.getText());

        XmlWriteParser.Payload unclosed = XmlWriteParser.parse(hexSql(bytes, false));
        Assertions.assertArrayEquals(bytes, unclosed.getBytes());
    }

    private static void assertHexSplit(String text, int splitAt) throws Exception {
        byte[] utf8 = text.getBytes(StandardCharsets.UTF_8);
        Assertions.assertTrue(utf8.length > splitAt);
        assertHexSplit(
                text,
                Arrays.copyOfRange(utf8, 0, splitAt),
                Arrays.copyOfRange(utf8, splitAt, utf8.length));
    }

    private static void assertHexSplit(String text, byte[] left, byte[] right) throws Exception {
        String decodedApart =
                new String(left, StandardCharsets.UTF_8)
                        + new String(right, StandardCharsets.UTF_8);
        Assertions.assertTrue(decodedApart.indexOf('\uFFFD') >= 0);

        XmlWriteParser.Payload leftPayload = XmlWriteParser.parse(hexSql(left, true));
        XmlWriteParser.Payload rightPayload = XmlWriteParser.parse(hexSql(right, true));
        XmlUnderConstruction xml = new XmlUnderConstruction();
        xml.add(new XmlFragment(bytesEvent(leftPayload.getBytes())));
        xml.add(new XmlFragment(bytesEvent(rightPayload.getBytes())));

        String merged = (String) xml.merge();
        Assertions.assertEquals(text, merged);
        Assertions.assertEquals(-1, merged.indexOf('\uFFFD'));
    }

    private static String hexSql(byte[] bytes, boolean closeQuote) {
        StringBuilder hex = new StringBuilder();
        for (byte value : bytes) {
            hex.append(String.format("%02X", value & 0xff));
        }
        if (closeQuote) {
            return "XML_REDO := HEXTORAW('" + hex + "'):" + bytes.length;
        }
        return "XML_REDO := HEXTORAW('" + hex + "):" + bytes.length;
    }

    private static XmlWriteEvent bytesEvent(byte[] bytes) {
        return new XmlWriteEvent(
                EventType.XML_WRITE,
                Scn.valueOf(1),
                TABLE,
                "AAABBB",
                "0x1.2.3",
                Instant.EPOCH,
                bytes,
                bytes.length);
    }
}
