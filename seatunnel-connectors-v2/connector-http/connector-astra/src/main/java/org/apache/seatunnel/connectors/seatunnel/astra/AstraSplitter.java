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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Applies literal, Markdown or language-aware split preferences subject to a true token ceiling.
 * Unicode characters stay intact and overlap always advances the next chunk.
 */
final class AstraSplitter {
    private static final int MAX_BOUNDARIES = 65536;
    private static final Pattern HEADINGS = Pattern.compile("(?m)^ {0,3}(#{1,6})[ \\t]+");
    private static final Map<String, String> CODE =
            Map.ofEntries(
                    Map.entry("cpp", "namespace|class|struct|enum|template|void|int"),
                    Map.entry("go", "package|import|func|type|struct|interface"),
                    Map.entry(
                            "java",
                            "package|import|public|protected|private|class|interface|enum|record"),
                    Map.entry("js", "export|import|function|async|class|const|let"),
                    Map.entry("php", "<\\?php|namespace|use|class|interface|trait|function"),
                    Map.entry("proto", "syntax|package|import|message|service|enum|rpc"),
                    Map.entry("python", "class|def|async|from|import|@"),
                    Map.entry("rst", "\\.\\.|={3,}|-{3,}"),
                    Map.entry("ruby", "class|module|def|require|include"),
                    Map.entry("rust", "pub|impl|mod|struct|enum|trait|fn|use"),
                    Map.entry("scala", "package|import|object|class|trait|given|def|val"),
                    Map.entry("swift", "import|class|struct|enum|protocol|extension|func"),
                    Map.entry("markdown", "#{1,6}|\\x60{3,}|~{3,}"),
                    Map.entry("latex", "\\\\(?:chapter|section|subsection|begin|end)"),
                    Map.entry("html", "</?(?:html|body|main|section|article|div|p|h[1-6])"),
                    Map.entry(
                            "sol",
                            "pragma|import|contract|library|interface|function|event|struct"));

    private final AstraConfig config;
    private final AstraHttp transport;
    private AstraTokenizer tokenizer;

    AstraSplitter(AstraConfig config, AstraHttp transport) {
        this.config = config;
        this.transport = transport;
    }

    static boolean supportedLanguage(String language) {
        return CODE.containsKey(language);
    }

    /**
     * Emits only complete characters and checks every result against the configured GPT-2 limit.
     * Oversized indivisible characters or excessive chunk expansion fail before any document write.
     */
    List<String> split(String input) throws IOException {
        String text = input.strip();
        if (text.isEmpty()) {
            return List.of();
        }
        byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
        if (bytes.length > config.maxRow) {
            throw failure("projected text exceeds max_row_bytes");
        }
        if (bytes.length <= config.chunkSize) {
            // No byte-pair encoding yields more than one token per byte.
            return List.of(text);
        }
        if (tokenizer == null) {
            tokenizer = AstraTokenizer.load(config, transport);
        }
        long[] work = {Math.max(65536L, text.length() * 96L)};
        if (fits(text, 0, text.length(), config.chunkSize, work)) {
            return List.of(text);
        }
        List<NavigableMap<Integer, Integer>> preferred = boundaries(text);
        List<String> output = new ArrayList<>();
        int start = 0;
        while (start < text.length()) {
            if (output.size() == config.maxChunks) {
                throw failure("row exceeds max_chunks_per_row");
            }
            int end = fittingPrefix(text, start, work);
            int next = end;
            if (end < text.length()) {
                for (NavigableMap<Integer, Integer> choices : preferred) {
                    Map.Entry<Integer, Integer> choice = choices.floorEntry(end);
                    if (choice != null
                            && choice.getKey() > start
                            && !text.substring(start, choice.getKey()).isBlank()
                            && fits(text, start, choice.getKey(), config.chunkSize, work)) {
                        end = choice.getKey();
                        next = choice.getValue();
                        break;
                    }
                }
            }
            String chunk = text.substring(start, end).strip();
            if (!chunk.isEmpty()) {
                output.add(chunk);
            }
            if (next == text.length()) {
                break;
            }
            if (config.overlap > 0 && !chunk.isEmpty()) {
                next = suffix(text, start, end, next, work);
            }
            if (next <= start) {
                throw failure("splitter could not advance");
            }
            start = next;
        }
        return output;
    }

    private int fittingPrefix(String text, int start, long[] work) throws IOException {
        int first = start + Character.charCount(text.codePointAt(start));
        if (!fits(text, start, first, config.chunkSize, work)) {
            throw failure("chunk_size is too small for a complete Unicode character");
        }
        int max = Math.min(text.length(), start + config.chunkSize * tokenizer.maxTokenBytes());
        int lo = first;
        int hi = max;
        int good = first;
        while (lo <= hi) {
            int middle = beforeCodePoint(text, (lo + hi) >>> 1);
            if (middle < lo) {
                middle += 2;
            }
            if (middle > hi) {
                break;
            }
            if (fits(text, start, middle, config.chunkSize, work)) {
                good = middle;
                lo = middle + 1;
            } else {
                hi = middle - 1;
            }
        }
        return good;
    }

    private int suffix(String text, int start, int end, int next, long[] work) throws IOException {
        int lo = start + Character.charCount(text.codePointAt(start));
        int hi = end - 1;
        int found = next;
        while (lo <= hi) {
            int middle = beforeCodePoint(text, (lo + hi) >>> 1);
            if (middle < lo) {
                middle += 2;
            }
            if (middle > hi) {
                break;
            }
            if (fits(text, middle, end, config.overlap, work)) {
                found = middle;
                hi = middle - 1;
            } else {
                lo = middle + 1;
            }
        }
        return found;
    }

    private boolean fits(String text, int from, int through, int tokens, long[] work)
            throws IOException {
        work[0] -= through - from;
        if (work[0] < 0) {
            throw failure("row exceeds the tokenization work budget");
        }
        return tokenizer.countWithin(text.substring(from, through).strip(), tokens) <= tokens;
    }

    private static int beforeCodePoint(String text, int offset) {
        return offset < text.length()
                        && Character.isLowSurrogate(text.charAt(offset))
                        && offset > 0
                        && Character.isHighSurrogate(text.charAt(offset - 1))
                ? offset - 1
                : offset;
    }

    private List<NavigableMap<Integer, Integer>> boundaries(String text) throws IOException {
        List<NavigableMap<Integer, Integer>> choice = new ArrayList<>();
        int[] remaining = {MAX_BOUNDARIES};
        switch (config.splitter) {
            case MARKDOWN:
                addMarkdown(text, choice, remaining);
                addLiteral(text, "\n\n", true, choice, remaining);
                addLiteral(text, "\n", true, choice, remaining);
                break;
            case CODE:
                Pattern definitions =
                        Pattern.compile(
                                "(?m)^[ \\t]*(?:" + CODE.get(config.codeLanguage) + ")(?=\\W|$)");
                NavigableMap<Integer, Integer> structurals = new TreeMap<>();
                Matcher matcher = definitions.matcher(text);
                while (matcher.find()) {
                    boundaryBudget(remaining);
                    structurals.put(matcher.start(), matcher.start());
                }
                choice.add(structurals);
                addLiteral(text, "\n\n", true, choice, remaining);
                addLiteral(text, "\n", true, choice, remaining);
                break;
            default:
                for (String separator : config.separators) {
                    if (!separator.isEmpty()) {
                        addLiteral(text, separator, config.keepSeparator, choice, remaining);
                    }
                }
        }
        return choice;
    }

    private void addMarkdown(
            String text, List<NavigableMap<Integer, Integer>> choice, int[] remaining)
            throws IOException {
        List<NavigableMap<Integer, Integer>> levels = new ArrayList<>();
        for (int level = 0; level < config.markdownLevel; level++) {
            levels.add(new TreeMap<>());
        }
        Matcher headings = HEADINGS.matcher(text);
        while (headings.find()) {
            int level = headings.group(1).length();
            if (level <= config.markdownLevel) {
                boundaryBudget(remaining);
                levels.get(level - 1).put(headings.start(), headings.start());
            }
        }
        choice.addAll(levels);
    }

    private static void addLiteral(
            String text,
            String separator,
            boolean keep,
            List<NavigableMap<Integer, Integer>> output,
            int[] remaining)
            throws IOException {
        NavigableMap<Integer, Integer> choices = new TreeMap<>();
        int at = 0;
        while ((at = text.indexOf(separator, at)) >= 0) {
            boundaryBudget(remaining);
            int after = at + separator.length();
            choices.put(at, keep ? at : after);
            at = after;
        }
        output.add(choices);
    }

    private static void boundaryBudget(int[] remaining) throws IOException {
        if (--remaining[0] < 0) {
            throw failure("row exceeds the number of preferred split boundaries");
        }
    }

    private static IOException failure(String message) {
        return new IOException("Astra sink: " + message);
    }
}
