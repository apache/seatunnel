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

package org.apache.seatunnel.connectors.seatunnel.file.reader;

import org.apache.seatunnel.connectors.seatunnel.file.source.reader.TextReadStrategy;

import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StreamLineSplitterTest {

    @Test
    void testDefaultLineDelimiterWithReadLine() throws IOException {
        String input = "line1\nline2\nline3\n";
        List<String> lines = new ArrayList<>();

        TextReadStrategy.StreamLineSplitter splitter =
                new TextReadStrategy.StreamLineSplitter("\n", 0, lines::add);
        try (BufferedReader reader = new BufferedReader(new StringReader(input))) {
            splitter.processStream(reader);
        }

        assertEquals(3, lines.size());
        assertEquals("line1", lines.get(0));
        assertEquals("line2", lines.get(1));
        assertEquals("line3", lines.get(2));
    }

    @Test
    void testDefaultLineDelimiterWithSkipHeader() throws IOException {
        String input = "header1\nheader2\nline1\nline2\nline3\n";
        List<String> lines = new ArrayList<>();

        TextReadStrategy.StreamLineSplitter splitter =
                new TextReadStrategy.StreamLineSplitter("\n", 2, lines::add);
        try (BufferedReader reader = new BufferedReader(new StringReader(input))) {
            splitter.processStream(reader);
        }

        assertEquals(3, lines.size());
        assertEquals("line1", lines.get(0));
        assertEquals("line2", lines.get(1));
        assertEquals("line3", lines.get(2));
    }

    @Test
    void testCustomDelimiter() throws IOException {
        String input = "line1|||line2|||line3|||";
        List<String> lines = new ArrayList<>();

        TextReadStrategy.StreamLineSplitter splitter =
                new TextReadStrategy.StreamLineSplitter("|||", 0, lines::add);
        try (BufferedReader reader = new BufferedReader(new StringReader(input))) {
            splitter.processStream(reader);
        }

        assertEquals(3, lines.size());
        assertEquals("line1", lines.get(0));
        assertEquals("line2", lines.get(1));
        assertEquals("line3", lines.get(2));
    }

    @Test
    void testCustomDelimiterWithSkipHeader() throws IOException {
        String input = "header1|||header2|||line1|||line2|||line3|||";
        List<String> lines = new ArrayList<>();

        TextReadStrategy.StreamLineSplitter splitter =
                new TextReadStrategy.StreamLineSplitter("|||", 2, lines::add);
        try (BufferedReader reader = new BufferedReader(new StringReader(input))) {
            splitter.processStream(reader);
        }

        assertEquals(3, lines.size());
        assertEquals("line1", lines.get(0));
        assertEquals("line2", lines.get(1));
        assertEquals("line3", lines.get(2));
    }

    @Test
    void testEmptyLines() throws IOException {
        String input = "line1\n\nline2\n  \nline3\n";
        List<String> lines = new ArrayList<>();

        TextReadStrategy.StreamLineSplitter splitter =
                new TextReadStrategy.StreamLineSplitter("\n", 0, lines::add);
        try (BufferedReader reader = new BufferedReader(new StringReader(input))) {
            splitter.processStream(reader);
        }

        assertEquals(3, lines.size());
        assertEquals("line1", lines.get(0));
        assertEquals("line2", lines.get(1));
        assertEquals("line3", lines.get(2));
    }

    @Test
    void testPartialDelimiter() throws IOException {
        String input = "line1||line2|||line3|||";
        List<String> lines = new ArrayList<>();

        TextReadStrategy.StreamLineSplitter splitter =
                new TextReadStrategy.StreamLineSplitter("|||", 0, lines::add);
        try (BufferedReader reader = new BufferedReader(new StringReader(input))) {
            splitter.processStream(reader);
        }

        assertEquals(2, lines.size());
        assertEquals("line1||line2", lines.get(0));
        assertEquals("line3", lines.get(1));
    }

    @Test
    void testEmptyInput() throws IOException {
        String input = "";
        List<String> lines = new ArrayList<>();

        TextReadStrategy.StreamLineSplitter splitter =
                new TextReadStrategy.StreamLineSplitter("\n", 0, lines::add);
        try (BufferedReader reader = new BufferedReader(new StringReader(input))) {
            splitter.processStream(reader);
        }

        assertTrue(lines.isEmpty());
    }

    @Test
    void testOnlyDelimiters() throws IOException {
        String input = "|||";
        List<String> lines = new ArrayList<>();

        TextReadStrategy.StreamLineSplitter splitter =
                new TextReadStrategy.StreamLineSplitter("|||", 0, lines::add);
        try (BufferedReader reader = new BufferedReader(new StringReader(input))) {
            splitter.processStream(reader);
        }

        assertEquals(0, lines.size());
    }

    @Test
    void testCarriageReturnLineFeed() throws IOException {
        String input = "line1\r\nline2\r\nline3\r\n";
        List<String> lines = new ArrayList<>();

        TextReadStrategy.StreamLineSplitter splitter =
                new TextReadStrategy.StreamLineSplitter("\r\n", 0, lines::add);
        try (BufferedReader reader = new BufferedReader(new StringReader(input))) {
            splitter.processStream(reader);
        }

        assertEquals(3, lines.size());
        assertEquals("line1", lines.get(0));
        assertEquals("line2", lines.get(1));
        assertEquals("line3", lines.get(2));
    }

    /** Be consistent with the previous behavior */
    @Test
    void testMixedDelimiters() throws IOException {
        String input = "line1\nline2\r\nline3\n";
        List<String> lines = new ArrayList<>();

        TextReadStrategy.StreamLineSplitter splitter =
                new TextReadStrategy.StreamLineSplitter("\n", 0, lines::add);
        try (BufferedReader reader = new BufferedReader(new StringReader(input))) {
            splitter.processStream(reader);
        }

        System.out.println("Actual lines: " + lines);
        for (int i = 0; i < lines.size(); i++) {
            System.out.println("Line " + i + ": '" + lines.get(i) + "'");
        }

        assertEquals(3, lines.size());
        assertEquals("line1", lines.get(0));
        assertEquals("line2", lines.get(1));
        assertEquals("line3", lines.get(2));
    }

    @Test
    void testLargeInput() throws IOException {
        StringBuilder input = new StringBuilder();
        List<String> expectedLines = new ArrayList<>();

        for (int i = 0; i < 1000; i++) {
            String line = "line" + i;
            input.append(line).append("\n");
            expectedLines.add(line);
        }

        List<String> actualLines = new ArrayList<>();
        TextReadStrategy.StreamLineSplitter splitter =
                new TextReadStrategy.StreamLineSplitter("\n", 0, actualLines::add);
        try (BufferedReader reader = new BufferedReader(new StringReader(input.toString()))) {
            splitter.processStream(reader);
        }

        assertEquals(expectedLines.size(), actualLines.size());
        for (int i = 0; i < expectedLines.size(); i++) {
            assertEquals(expectedLines.get(i), actualLines.get(i));
        }
    }
}
