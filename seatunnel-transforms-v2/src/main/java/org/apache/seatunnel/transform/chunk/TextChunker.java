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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Pure text-chunking algorithm.
 *
 * <ul>
 *   <li>null/empty text produces no chunks;
 *   <li>empty {@code separators} falls back to fixed-size splitting with overlap;
 *   <li>otherwise splits recursively by separators (priority order), then greedily merges the
 *       pieces up to {@code chunkSize}.
 * </ul>
 *
 * <p>All lengths ({@code chunkSize} and {@code overlapSize}) are measured in <b>Unicode code
 * points</b>, not UTF-16 code units, so a supplementary character (e.g. an emoji) counts as one and
 * is never split across chunks.
 *
 * <p>Guarantees:
 *
 * <ul>
 *   <li>each chunk holds {@code <= chunkSize} code points (carried overlap counts toward the
 *       budget);
 *   <li>in the separator path, overlap is composed of whole trailing pieces, so it never starts
 *       mid-word; {@code overlapSize} is an upper bound and rounds down to whole pieces. In the
 *       empty-separators fixed-size fallback there are no pieces, so overlap is a plain window of
 *       {@code overlapSize} code points and is not rounded;
 *   <li>separators are retained on the piece they follow, so with {@code overlapSize == 0} the
 *       chunks concatenate back to the original text;
 *   <li>boundaries align to whole Unicode code points, so a surrogate pair (e.g. an emoji) is never
 *       split.
 * </ul>
 */
public final class TextChunker {

    private TextChunker() {}

    public static List<String> split(
            String text, List<String> separators, int chunkSize, int overlapSize) {
        if (text == null || text.isEmpty()) {
            return Collections.emptyList();
        }
        if (separators == null || separators.isEmpty()) {
            return fixedSize(text, chunkSize, overlapSize);
        }
        List<String> segments = recursiveSplit(text, separators, 0, chunkSize);
        return mergeWithOverlap(segments, chunkSize, overlapSize);
    }

    private static List<String> recursiveSplit(
            String text, List<String> separators, int index, int chunkSize) {
        List<String> result = new ArrayList<>();
        String separator = separators.get(index);
        List<String> parts =
                separator.isEmpty()
                        ? Collections.singletonList(text)
                        : splitByLiteral(text, separator);
        for (String part : parts) {
            if (part.isEmpty()) {
                continue;
            }
            if (codePointLength(part) <= chunkSize) {
                result.add(part);
            } else if (index + 1 < separators.size()) {
                result.addAll(recursiveSplit(part, separators, index + 1, chunkSize));
            } else {
                result.addAll(fixedSize(part, chunkSize, 0));
            }
        }
        return result;
    }

    private static List<String> splitByLiteral(String text, String separator) {
        List<String> parts = new ArrayList<>();
        int start = 0;
        int idx;
        while ((idx = text.indexOf(separator, start)) >= 0) {
            int next = idx + separator.length();
            parts.add(text.substring(start, next));
            start = next;
        }
        if (start < text.length()) {
            parts.add(text.substring(start));
        }
        return parts;
    }

    private static List<String> mergeWithOverlap(
            List<String> segments, int chunkSize, int overlapSize) {
        List<String> chunks = new ArrayList<>();
        List<String> current = new ArrayList<>();
        int currentLen = 0;
        for (String segment : segments) {
            int segmentLen = codePointLength(segment);
            if (currentLen > 0 && currentLen + segmentLen > chunkSize) {
                chunks.add(String.join("", current));
                current =
                        trailingSegmentsWithin(
                                current, Math.min(overlapSize, chunkSize - segmentLen));
                currentLen = 0;
                for (String s : current) {
                    currentLen += codePointLength(s);
                }
            }
            current.add(segment);
            currentLen += segmentLen;
        }
        if (currentLen > 0) {
            chunks.add(String.join("", current));
        }
        return chunks;
    }

    /**
     * Returns the longest suffix of {@code segments} whose combined length does not exceed {@code
     * budget}.
     */
    private static List<String> trailingSegmentsWithin(List<String> segments, int budget) {
        int total = 0;
        int from = segments.size();
        for (int i = segments.size() - 1; i >= 0; i--) {
            int len = codePointLength(segments.get(i));
            if (total + len > budget) {
                break;
            }
            total += len;
            from = i;
        }
        return new ArrayList<>(segments.subList(from, segments.size()));
    }

    private static List<String> fixedSize(String text, int chunkSize, int overlapSize) {
        List<String> chunks = new ArrayList<>();
        int len = text.length();
        int stepCodePoints = Math.max(1, chunkSize - overlapSize);
        int start = 0;
        while (start < len) {
            int end = advanceCodePoints(text, start, chunkSize);
            chunks.add(text.substring(start, end));
            if (end >= len) {
                break;
            }
            start = advanceCodePoints(text, start, stepCodePoints);
        }
        return chunks;
    }

    private static int codePointLength(String s) {
        return s.codePointCount(0, s.length());
    }

    private static int advanceCodePoints(String text, int start, int codePoints) {
        int index = start;
        int len = text.length();
        for (int c = 0; c < codePoints && index < len; c++) {
            index += Character.charCount(text.codePointAt(index));
        }
        return index;
    }
}
