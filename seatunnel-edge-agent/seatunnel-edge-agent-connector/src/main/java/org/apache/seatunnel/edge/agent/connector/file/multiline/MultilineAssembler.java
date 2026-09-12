/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.edge.agent.connector.file.multiline;

import lombok.Getter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

public class MultilineAssembler {

    /** Represents a single physical line with its metadata. */
    @Getter
    public static final class LineElement {
        private final String text;
        private final String filePath;
        private final long lineNumber;
        private final long offset;
        private final long ts;

        public LineElement(String text, String filePath, long lineNumber, long offset, long ts) {
            this.text = text;
            this.filePath = filePath;
            this.lineNumber = lineNumber;
            this.offset = offset;
            this.ts = ts;
        }
    }

    public enum MatchMode {
        /**
         * A line that matches the pattern is the first line of a <em>new</em> event. The previous
         * buffered lines (if any) are flushed as the completed prior event, then this line is
         * buffered as the start of the next event.
         */
        AFTER,
        /**
         * A line that matches the pattern is the <em>last</em> line of the current event. It is
         * appended to the buffer and the whole buffer is flushed immediately.
         */
        BEFORE
    }

    private final Pattern pattern;
    private final MatchMode matchMode;
    private final boolean negate;
    private final int maxLines;
    private final List<LineElement> buffer;

    /**
     * @param regex pattern applied to each line's text (via {@code Pattern.matcher} {@code
     *     .find()})
     * @param matchMode AFTER or BEFORE semantics (see {@link MatchMode})
     * @param negate if true, pattern match is inverted
     * @param maxLines maximum number of lines to retain in the buffer before forcing a flush (must
     *     be {@code >= 1})
     */
    public MultilineAssembler(String regex, MatchMode matchMode, boolean negate, int maxLines) {
        if (maxLines < 1) {
            throw new IllegalArgumentException("maxLines must be >= 1");
        }
        this.pattern = Pattern.compile(Objects.requireNonNull(regex, "regex"));
        this.matchMode = Objects.requireNonNull(matchMode, "matchMode");
        this.negate = negate;
        this.maxLines = maxLines;
        this.buffer = new ArrayList<>();
    }

    /**
     * Feeds one physical line into the assembler.
     *
     * @return a new list containing the completed prior event if this line triggered a flush, or
     *     {@code null} if the line was only buffered and no event completed yet
     */
    public List<LineElement> addLine(LineElement line) {
        boolean matches = pattern.matcher(line.getText()).find();
        if (negate) {
            matches = !matches;
        }

        List<LineElement> flushed = null;

        switch (matchMode) {
            case AFTER:
                if (matches) {
                    if (!buffer.isEmpty()) {
                        flushed = new ArrayList<>(buffer);
                        buffer.clear();
                    }
                }
                break;
            case BEFORE:
                if (matches) {
                    buffer.add(line);
                    flushed = new ArrayList<>(buffer);
                    buffer.clear();
                    return flushed;
                }
                break;
        }

        if (buffer.size() >= maxLines) {
            flushed = new ArrayList<>(buffer);
            buffer.clear();
        }

        buffer.add(line);
        return flushed;
    }

    /**
     * Flushes any remaining buffered lines as a final event (e.g. at EOF or end of poll batch).
     *
     * @return the buffered lines, or an empty immutable list if nothing is pending
     */
    public List<LineElement> flush() {
        if (buffer.isEmpty()) {
            return Collections.emptyList();
        }
        List<LineElement> flushed = new ArrayList<>(buffer);
        buffer.clear();
        return flushed;
    }

    /** @return {@code true} if there are lines waiting for a boundary or {@code flush()} */
    public boolean hasPending() {
        return !buffer.isEmpty();
    }

    /** @return timestamp of the first buffered line, or 0 if buffer is empty */
    public long getBufferFirstTimestamp() {
        return buffer.isEmpty() ? 0L : buffer.get(0).getTs();
    }
}
