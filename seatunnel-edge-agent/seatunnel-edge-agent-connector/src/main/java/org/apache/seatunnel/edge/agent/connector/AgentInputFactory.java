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

package org.apache.seatunnel.edge.agent.connector;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

/** Constructs {@link AgentInput} instances from a logical type and filesystem paths. */
public final class AgentInputFactory {

    public static final String TYPE_FILE = "file";
    public static final String TYPE_LOG = "log";
    public static final String TYPE_EVENT = "event";

    private AgentInputFactory() {}

    /**
     * Creates an {@link EventAgentInput} with no file paths (memory-only). Callers use {@link
     * EventAgentInput#append(String)}, {@link EventAgentInput#publish(String)}, and {@link
     * EventAgentInput#publish(List)} to supply records.
     */
    public static EventAgentInput createEvent() {
        return createEvent(Collections.emptyList());
    }

    /**
     * Creates an {@link EventAgentInput}.
     *
     * <ul>
     *   <li>If {@code paths} is null or empty after filtering null entries: memory-only mode.
     *   <li>Otherwise: on {@link AgentInput#open()}, all NDJSON lines from the paths are loaded
     *       into a queue (same order as {@link JsonLineSupport#loadAllRecords(List)}).
     * </ul>
     *
     * @param paths zero or more NDJSON files; null treated as empty
     */
    public static EventAgentInput createEvent(List<Path> paths) {
        return new EventAgentInput(paths);
    }

    /**
     * Creates an input for the given type.
     *
     * <ul>
     *   <li>{@link #TYPE_FILE}: requires at least one path; reads NDJSON lines sequentially across
     *       files.
     *   <li>{@link #TYPE_LOG}: requires exactly one path; follows the log file. By default new
     *       bytes are read from end-of-file (similar to {@code tail -f}). Use {@link
     *       #create(String, List, boolean)} to read from the beginning.
     *   <li>{@link #TYPE_EVENT}: same as {@link #createEvent(List)} — empty paths enable
     *       memory-only injection via {@link EventAgentInput#append(String)}, {@link
     *       EventAgentInput#publish(String)}, and {@link EventAgentInput#publish(List)}; non-empty
     *       paths preload files at {@link AgentInput#open()}.
     * </ul>
     *
     * @param type input discriminator ({@link #TYPE_FILE}, {@link #TYPE_LOG}, {@link #TYPE_EVENT})
     * @param paths file paths; interpretation depends on {@code type}; null treated as empty list
     */
    public static AgentInput create(String type, List<Path> paths) {
        return create(type, paths, false);
    }

    /**
     * Same as {@link #create(String, List)} but allows choosing whether a log input starts at the
     * beginning of the file or at the current end (tail).
     *
     * @param logReadFromBeginning when {@code type} is {@link #TYPE_LOG}, if {@code true} read from
     *     offset 0; if {@code false} start at EOF for incremental reads
     */
    public static AgentInput create(String type, List<Path> paths, boolean logReadFromBeginning) {
        String normalizedType = normalizeType(type);
        List<Path> safePaths = JsonLineSupport.nonNullPaths(paths);
        if (TYPE_FILE.equals(normalizedType)) {
            if (safePaths.isEmpty()) {
                throw new IllegalArgumentException(
                        "file input requires at least one path in paths");
            }
            return new FileAgentInput(new ArrayList<>(safePaths));
        }
        if (TYPE_LOG.equals(normalizedType)) {
            if (safePaths.size() != 1) {
                throw new IllegalArgumentException("log input requires exactly one path");
            }
            return new LogAgentInput(safePaths.get(0), logReadFromBeginning);
        }
        if (TYPE_EVENT.equals(normalizedType)) {
            return new EventAgentInput(safePaths);
        }
        throw new IllegalArgumentException(
                "unsupported type: "
                        + type
                        + " (expected one of "
                        + TYPE_FILE
                        + ", "
                        + TYPE_LOG
                        + ", "
                        + TYPE_EVENT
                        + ")");
    }

    private static String normalizeType(String type) {
        if (type == null || type.trim().isEmpty()) {
            throw new IllegalArgumentException("type must be non-empty");
        }
        return type.trim().toLowerCase(Locale.ROOT);
    }
}
