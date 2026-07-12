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

package org.apache.seatunnel.transform.chunk;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

class TextChunkerTest {

    private static final List<String> SEPARATORS = Arrays.asList("\n\n", "\n", " ");

    @Test
    void nullOrEmptyProducesNoChunks() {
        Assertions.assertTrue(TextChunker.split(null, SEPARATORS, 100, 0).isEmpty());
        Assertions.assertTrue(TextChunker.split("", SEPARATORS, 100, 0).isEmpty());
    }

    @Test
    void shortTextStaysSingleChunk() {
        List<String> chunks = TextChunker.split("hello world", SEPARATORS, 100, 0);
        Assertions.assertEquals(Collections.singletonList("hello world"), chunks);
    }

    @Test
    void everyChunkWithinChunkSize() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 50; i++) {
            sb.append("para-").append(i).append("\n\n");
        }
        List<String> chunks = TextChunker.split(sb.toString(), SEPARATORS, 40, 0);
        Assertions.assertFalse(chunks.isEmpty());
        chunks.forEach(c -> Assertions.assertTrue(c.length() <= 40, "chunk too long: " + c));
    }

    @Test
    void emptySeparatorsFallsBackToFixedSize() {
        List<String> chunks = TextChunker.split("abcdefghij", Collections.emptyList(), 4, 0);
        Assertions.assertEquals(Arrays.asList("abcd", "efgh", "ij"), chunks);
    }

    @Test
    void fixedSizeHonorsOverlap() {
        List<String> chunks = TextChunker.split("abcdefghij", Collections.emptyList(), 4, 1);
        Assertions.assertEquals(Arrays.asList("abcd", "defg", "ghij"), chunks);
    }

    @Test
    void longUnbreakableSegmentIsHardSplit() {
        // single 25-char token, no usable separator -> hard-split to <= chunkSize
        String token = "abcdefghijklmnopqrstuvwxy";
        List<String> chunks = TextChunker.split(token, SEPARATORS, 10, 0);
        chunks.forEach(c -> Assertions.assertTrue(c.length() <= 10));
        Assertions.assertEquals(token, String.join("", chunks));
    }

    @Test
    void overlapCarriesWholeWordsNotCharacterFragments() {
        List<String> chunks = TextChunker.split("aa bb cc dd", Arrays.asList(" "), 6, 3);
        chunks.forEach(c -> Assertions.assertTrue(c.length() <= 6, "chunk too long: " + c));
        Assertions.assertEquals(Arrays.asList("aa bb ", "bb cc ", "cc dd"), chunks);
        Assertions.assertTrue(chunks.get(1).startsWith("bb "));
    }

    @Test
    void overlapDegradesToEmptyWhenAWholeWordExceedsBudget() {
        List<String> chunks = TextChunker.split("aa bb cc dd", Arrays.asList(" "), 5, 2);
        chunks.forEach(c -> Assertions.assertTrue(c.length() <= 5, "chunk too long: " + c));
        Assertions.assertEquals(Arrays.asList("aa ", "bb ", "cc dd"), chunks);
    }

    @Test
    void separatorsAndBlankLinesAreRetainedSoChunkingIsLossless() {
        List<String> chunks = TextChunker.split("aa bb\n\ncc dd", Arrays.asList("\n\n", " "), 6, 0);
        chunks.forEach(c -> Assertions.assertTrue(c.length() <= 6, "chunk too long: " + c));
        Assertions.assertEquals(Arrays.asList("aa ", "bb\n\n", "cc dd"), chunks);
        Assertions.assertEquals("aa bb\n\ncc dd", String.join("", chunks));
    }

    @Test
    void fixedSizeSplitDoesNotBreakSurrogatePairs() {
        String emoji = "😀";
        String text = emoji + emoji + emoji; // 3 code points, 6 chars
        // chunkSize=3 would cut the second emoji mid-pair without code-point alignment
        List<String> chunks = TextChunker.split(text, Collections.emptyList(), 3, 0);
        chunks.forEach(TextChunkerTest::assertNoDanglingSurrogate);
        Assertions.assertEquals(text, String.join("", chunks)); // no overlap -> lossless
    }

    private static void assertNoDanglingSurrogate(String s) {
        for (int i = 0; i < s.length(); i++) {
            char ch = s.charAt(i);
            if (Character.isHighSurrogate(ch)) {
                Assertions.assertTrue(
                        i + 1 < s.length() && Character.isLowSurrogate(s.charAt(i + 1)),
                        "dangling high surrogate in chunk: " + s);
                i++;
            } else {
                Assertions.assertFalse(
                        Character.isLowSurrogate(ch), "dangling low surrogate in chunk: " + s);
            }
        }
    }
}
