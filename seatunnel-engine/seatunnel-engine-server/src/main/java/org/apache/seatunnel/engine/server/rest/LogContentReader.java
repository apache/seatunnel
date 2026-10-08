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

package org.apache.seatunnel.engine.server.rest;

import org.apache.seatunnel.common.utils.FileUtils;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Locale;

/**
 * Reads the log-file content the REST log endpoints return, bounded by {@code
 * log-response-max-size-mb}.
 *
 * <p>Shared by the v1 handler and the v2 servlets so that the two describe a truncated log the same
 * way.
 */
public final class LogContentReader {

    /**
     * Opening line of a truncated response. Written as a complete line so that a consumer reading
     * the body line by line can recognise and drop it.
     */
    private static final String TRUNCATION_NOTICE_FORMAT =
            "[SeaTunnel] Log truncated: returning %d bytes from the tail of %d bytes"
                    + " (file size at read start). A partial first line is omitted when possible;"
                    + " an oversized single line returns a UTF-8-safe partial tail."
                    + " Raise seatunnel.engine.http.log-response-max-size-mb, or set"
                    + " it to 0 for no limit, to return more.\n";

    private LogContentReader() {}

    /**
     * Reads the content to return for a log file.
     *
     * <p>A positive {@code maxBytes} bounds the file content read into memory. A larger file is
     * represented by its tail.
     *
     * <p>A truncated response opens with a notice naming the retained bytes and the file size
     * captured by the same read. The tail starts at a complete line when possible, or at a UTF-8
     * character boundary for an oversized single line. The notice lets readers and archiving
     * scripts distinguish the retained tail from a complete log.
     *
     * @param path canonical path of the log file, already resolved against the log directory
     * @param maxBytes cap from {@code HttpConfig#getLogResponseMaxSizeBytes()}; <= 0 means no limit
     * @return the log content, opening with a truncation notice when the file exceeded the cap
     * @throws IOException if the response content cannot be decoded
     */
    public static String read(Path path, long maxBytes) throws IOException {
        return read(FileUtils.readFileTail(path, maxBytes));
    }

    static String read(FileUtils.FileTail tail) throws IOException {
        if (!tail.isTruncated()) {
            return tail.getContent();
        }
        String notice =
                String.format(
                        Locale.ROOT,
                        TRUNCATION_NOTICE_FORMAT,
                        tail.getReturnedBytes(),
                        tail.getFileSize());
        return tail.getContentWithPrefix(notice);
    }
}
