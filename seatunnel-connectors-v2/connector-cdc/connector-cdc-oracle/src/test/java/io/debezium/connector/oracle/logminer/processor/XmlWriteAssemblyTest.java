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
import io.debezium.connector.oracle.logminer.parser.XmlParserUtils;
import io.debezium.connector.oracle.logminer.parser.XmlWriteParser;
import io.debezium.connector.oracle.logminer.processor.TransactionCommitConsumer.XmlFragment;
import io.debezium.connector.oracle.logminer.processor.TransactionCommitConsumer.XmlUnderConstruction;
import io.debezium.relational.TableId;
import oracle.sql.CharacterSet;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;

public class XmlWriteAssemblyTest {

    private static final TableId TABLE = TableId.parse("DEBEZIUM.XML_TEST");
    /** Debezium treats INFO ending with this marker as binary XML_WRITE. */
    private static final String BINARY_INFO = "XML DOC write not re-executable";

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
    public void decodesLatin1HexChunksWithTheDatabaseCharacterSetAfterAssembly() {
        // "<root>Ä</root>" in WE8ISO8859P1; Ä is 0xC4, which is not valid UTF-8 alone.
        byte[] document =
                new byte[] {
                    0x3c,
                    0x72,
                    0x6f,
                    0x6f,
                    0x74,
                    0x3e,
                    (byte) 0xc4,
                    0x3c,
                    0x2f,
                    0x72,
                    0x6f,
                    0x6f,
                    0x74,
                    0x3e
                };
        CharacterSet latin1 = CharacterSet.make(CharacterSet.WE8ISO8859P1_CHARSET);
        XmlUnderConstruction xml = new XmlUnderConstruction(latin1);
        xml.add(new XmlFragment(bytesEvent(Arrays.copyOfRange(document, 0, 7))));
        xml.add(new XmlFragment(bytesEvent(Arrays.copyOfRange(document, 7, document.length))));
        Assertions.assertEquals("<root>\u00C4</root>", xml.merge());
    }

    @Test
    public void keepsASurrogatePairSplitAcrossQuotedChunks() {
        XmlUnderConstruction xml = new XmlUnderConstruction();
        xml.add(new XmlFragment("\uD83D"));
        xml.add(new XmlFragment("\uDE00"));

        Assertions.assertEquals("\uD83D\uDE00", xml.merge());
    }

    @Test
    public void parsesDocumentTextWithoutTheXmlRedoPreamble() throws Exception {
        String first = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><note>it's track</note>";
        String second = "<title>现场 \uD83D\uDE00</title>";
        XmlWriteParser.Payload start = XmlWriteParser.parse(first, "");
        XmlWriteParser.Payload rest = XmlWriteParser.parse(second, "");
        Assertions.assertEquals(first, start.getText());
        Assertions.assertNull(start.getBytes());

        XmlUnderConstruction xml = new XmlUnderConstruction();
        xml.add(new XmlFragment(start.getText()));
        xml.add(new XmlFragment(rest.getText()));
        Assertions.assertEquals(first + second, xml.merge());
    }

    @Test
    public void parsesInlineXmlAsTextAndNullAsNull() throws Exception {
        XmlWriteParser.Payload text =
                XmlWriteParser.parse("XML_REDO := '<a>数符癸</a>':10", BINARY_INFO);
        Assertions.assertEquals("<a>数符癸</a>", text.getText());
        Assertions.assertNull(text.getBytes());
        Assertions.assertEquals(10, text.getLength());

        XmlWriteParser.Payload nullXml = XmlWriteParser.parse("XML_REDO := NULL", "");
        Assertions.assertTrue(nullXml.isNullValue());
        Assertions.assertNull(nullXml.getText());
        Assertions.assertNull(nullXml.getBytes());
    }

    @Test
    public void usesLogMinerInfoToChooseBinaryXmlWriteLikeDebezium() throws Exception {
        byte[] bytes = "é".getBytes(StandardCharsets.UTF_8);
        String sql = hexSql(bytes, true);

        XmlWriteParser.Payload asDocument = XmlWriteParser.parse(sql, "");
        Assertions.assertEquals(sql, asDocument.getText());
        Assertions.assertNull(asDocument.getBytes());

        XmlWriteParser.Payload binary = XmlWriteParser.parse(sql, BINARY_INFO);
        Assertions.assertArrayEquals(bytes, binary.getBytes());
        Assertions.assertNull(binary.getText());
    }

    @Test
    public void elevenGNullInfoTreatsRedoAsDocumentText() throws Exception {
        Assertions.assertFalse(XmlParserUtils.isXmlSerializedAsBinary((String) null));
        Assertions.assertFalse(XmlParserUtils.isXmlSerializedAsBinary("XML DOC WRITE"));
        Assertions.assertTrue(XmlParserUtils.isXmlSerializedAsBinary(BINARY_INFO));

        XmlWriteParser.Payload elevenG = XmlWriteParser.parse("<root>11g</root>", null);
        Assertions.assertEquals("<root>11g</root>", elevenG.getText());
        Assertions.assertNull(elevenG.getBytes());

        XmlWriteParser.Payload blank = XmlWriteParser.parse("", BINARY_INFO);
        Assertions.assertTrue(blank.isNullValue());
        Assertions.assertTrue(XmlWriteParser.parse(null, BINARY_INFO).isNullValue());
        Assertions.assertTrue(XmlWriteParser.parse("XML_REDO := NULL", BINARY_INFO).isNullValue());
    }

    @Test
    public void binaryXmlWriteRejectsMalformedRedo() {
        Assertions.assertThrows(
                IllegalStateException.class, () -> XmlWriteParser.parse("<root/>", BINARY_INFO));
        Assertions.assertThrows(
                IllegalStateException.class,
                () -> XmlWriteParser.parse("XML_REDO := HEXTORAW(not-hex):1", BINARY_INFO));
        // The only ':' in this SQL is the assignment operator, so length parsing fails.
        Assertions.assertThrows(
                NumberFormatException.class,
                () -> XmlWriteParser.parse("XML_REDO := HEXTORAW('41')", BINARY_INFO));
    }

    @Test
    public void binaryQuotedXmlRedoIsDocumentText() throws Exception {
        XmlWriteParser.Payload quoted =
                XmlWriteParser.parse("XML_REDO := '<note>it''s xml</note>':16", BINARY_INFO);
        Assertions.assertEquals("<note>it''s xml</note>", quoted.getText());
        Assertions.assertNull(quoted.getBytes());
        Assertions.assertEquals(16, quoted.getLength());
    }

    @Test
    public void decodeHexBytesTreatsEmptyAsEmptyAndNullCharsetAsUtf8() {
        Assertions.assertEquals("", XmlWriteParser.decodeHexBytes(null, null));
        Assertions.assertEquals("", XmlWriteParser.decodeHexBytes(new byte[0], null));
        Assertions.assertEquals("A", XmlWriteParser.decodeHexBytes(new byte[] {0x41}, null));
    }

    @Test
    public void parsesHexChunksAsBytesIncludingTheUnclosedQuoteForm() throws Exception {
        byte[] bytes = "é".getBytes(StandardCharsets.UTF_8);
        XmlWriteParser.Payload quoted = XmlWriteParser.parse(hexSql(bytes, true), BINARY_INFO);
        Assertions.assertArrayEquals(bytes, quoted.getBytes());
        Assertions.assertNull(quoted.getText());

        XmlWriteParser.Payload unclosed = XmlWriteParser.parse(hexSql(bytes, false), BINARY_INFO);
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

        XmlWriteParser.Payload leftPayload = XmlWriteParser.parse(hexSql(left, true), BINARY_INFO);
        XmlWriteParser.Payload rightPayload =
                XmlWriteParser.parse(hexSql(right, true), BINARY_INFO);
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
