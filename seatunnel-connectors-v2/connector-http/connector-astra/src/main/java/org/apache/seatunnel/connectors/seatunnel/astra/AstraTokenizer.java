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

package org.apache.seatunnel.connectors.seatunnel.astra;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Base64;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Counts GPT-2 tokens from its published byte-pair ranks and Unicode pre-tokenization rules. The
 * rank data is checksum-verified at load time and does not include application data.
 */
final class AstraTokenizer {
    static final URI RANK_URL =
            URI.create("https://openaipublic.blob.core.windows.net/encodings/r50k_base.tiktoken");
    static final int MAX_RANK_BYTES = 1048576;
    private static final int CORPUS_SIZE = 50256;
    private static final String RANK_SHA =
            "306cd27f03c1a714eca7108e03d66b7dc042abe8c258b44c199a7ed9838dd930";
    private static final Pattern WORD =
            Pattern.compile(
                    "'(?:s|t|re|ve|m|ll|d)| ?\\p{L}++| ?\\p{N}++| ?[^\\s\\p{L}\\p{N}]++|"
                            + "\\s++\\z|\\s+(?!\\S)|\\s",
                    Pattern.UNICODE_CHARACTER_CLASS);
    private static final String HEX = "0123456789abcdef";
    private static volatile AstraTokenizer official;
    private final Map<String, Integer> ranks;
    private final int maxTokenBytes;

    private AstraTokenizer(Map<String, Integer> ranks, int maxTokenBytes) {
        this.ranks = ranks;
        this.maxTokenBytes = maxTokenBytes;
    }

    /**
     * Loads a local rank file or obtains the pinned public copy without credentials. The verified
     * hosted dictionary is shared across writers through the connector classloader.
     */
    static AstraTokenizer load(AstraConfig options, AstraHttp transport) throws IOException {
        if (!options.tokenizerFile.isEmpty()) {
            byte[] body;
            try (InputStream source = Files.newInputStream(Path.of(options.tokenizerFile))) {
                body = source.readNBytes(MAX_RANK_BYTES + 1);
            } catch (IOException | RuntimeException ignored) {
                throw new IOException("Astra sink: cannot read tokenizer_data_file");
            }
            return parse(body);
        }
        AstraTokenizer present = official;
        if (present != null) {
            return present;
        }
        // A downloader may time out independently. Never hold the class lock during network I/O.
        AstraTokenizer loaded = parse(transport.getRanks(RANK_URL, MAX_RANK_BYTES));
        synchronized (AstraTokenizer.class) {
            if (official == null) {
                official = loaded;
            }
            return official;
        }
    }

    private static AstraTokenizer parse(byte[] data) throws IOException {
        if (data.length > MAX_RANK_BYTES || !RANK_SHA.equals(hex(AstraJson.sha256(data)))) {
            throw new IOException(
                    "Astra sink: tokenizer rank data checksum mismatch or size exceeded");
        }
        Map<String, Integer> ranks = new HashMap<>(CORPUS_SIZE * 2);
        int longest = 1;
        String source = new String(data, StandardCharsets.US_ASCII);
        for (String line : source.split("\n")) {
            int split = line.indexOf(' ');
            byte[] token = Base64.getDecoder().decode(line.substring(0, split));
            ranks.put(
                    new String(token, StandardCharsets.ISO_8859_1),
                    Integer.parseInt(line.substring(split + 1)));
            longest = Math.max(longest, token.length);
        }
        if (ranks.size() != CORPUS_SIZE) {
            throw new IOException(
                    "Astra sink: tokenizer rank data has an unexpected vocabulary size");
        }
        return new AstraTokenizer(ranks, longest);
    }

    private static String hex(byte[] bytes) {
        StringBuilder encoded = new StringBuilder(bytes.length * 2);
        for (byte value : bytes) {
            encoded.append(HEX.charAt((value & 255) >>> 4)).append(HEX.charAt(value & 15));
        }
        return encoded.toString();
    }

    /**
     * Returns the exact number of tokens up to the limit, or a sentinel above that limit. It can
     * reject long unbroken byte ranges early using the largest valid vocabulary token.
     */
    int countWithin(String input, int limit) {
        int total = 0;
        Matcher matcher = WORD.matcher(input);
        while (matcher.find()) {
            byte[] piece = matcher.group().getBytes(StandardCharsets.UTF_8);
            if (piece.length > (limit - total) * maxTokenBytes) {
                return limit + 1;
            }
            total += pieces(piece);
            if (total > limit) {
                return limit + 1;
            }
        }
        return total;
    }

    int maxTokenBytes() {
        return maxTokenBytes;
    }

    private int pieces(byte[] bytes) {
        int size = bytes.length;
        if (size <= 1 || ranks.containsKey(new String(bytes, StandardCharsets.ISO_8859_1))) {
            return 1;
        }
        int[] next = new int[size];
        int[] prior = new int[size];
        int[] end = new int[size];
        Arrays.fill(next, -1);
        PriorityQueue<Pair> candidates =
                new PriorityQueue<>(
                        Comparator.comparingInt((Pair pair) -> pair.rank)
                                .thenComparingInt(pair -> pair.left));
        for (int start = 0; start < size; start++) {
            end[start] = start + 1;
            prior[start] = start - 1;
            if (start + 1 < size) {
                next[start] = start + 1;
            }
        }
        for (int start = 0; start < size - 1; start++) {
            offer(bytes, start, next, end, candidates);
        }
        int remaining = size;
        while (!candidates.isEmpty()) {
            Pair pair = candidates.remove();
            int left = pair.left;
            int right = end[left];
            if (next[left] != right || right != pair.middle || end[right] != pair.end) {
                continue;
            }
            end[left] = pair.end;
            int after = next[right];
            next[left] = after;
            if (after >= 0) {
                prior[after] = left;
            }
            next[right] = -1;
            remaining--;
            offer(bytes, prior[left], next, end, candidates);
            offer(bytes, left, next, end, candidates);
        }
        return remaining;
    }

    private void offer(byte[] bytes, int left, int[] next, int[] end, PriorityQueue<Pair> queue) {
        if (left < 0 || next[left] < 0) {
            return;
        }
        int right = next[left];
        int last = end[right];
        int length = last - left;
        if (length <= maxTokenBytes) {
            Integer rank = ranks.get(new String(bytes, left, length, StandardCharsets.ISO_8859_1));
            if (rank != null) {
                queue.add(new Pair(left, right, last, rank));
            }
        }
    }

    /**
     * Stores one candidate merge and the boundaries used to discard obsolete candidates. Equal
     * ranks resolve from left to right.
     */
    private static final class Pair {
        final int left;
        final int middle;
        final int end;
        final int rank;

        Pair(int left, int middle, int end, int rank) {
            this.left = left;
            this.middle = middle;
            this.end = end;
            this.rank = rank;
        }
    }
}
