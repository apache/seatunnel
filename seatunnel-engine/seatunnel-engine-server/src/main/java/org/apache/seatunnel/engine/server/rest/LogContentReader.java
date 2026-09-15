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
import java.nio.file.Files;
import java.nio.file.Path;

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
            "[SeaTunnel] Log truncated: returning the last %d bytes of %d, starting at the first"
                    + " complete line. Raise seatunnel.engine.http.log-response-max-size-mb, or set"
                    + " it to 0 for no limit, to return more.\n";

    private LogContentReader() {}

    /**
     * Reads the content to return for a log file.
     *
     * <p>At most {@code maxBytes} of the file is read and a larger file is represented by its tail,
     * so that requesting the log of a long-running streaming job cannot exhaust the node's heap.
     *
     * <p>A truncated response opens with a notice naming the returned and the total size. The tail
     * begins at a clean line boundary, which makes a truncated log otherwise indistinguishable from
     * a complete one - both to someone reading it in the web UI and to a script archiving it - and
     * the omitted part is the beginning of the job, which is where the cause of a failure usually
     * is.
     *
     * @param path canonical path of the log file, already resolved against the log directory
     * @param maxBytes cap from {@code HttpConfig#getLogResponseMaxSizeBytes()}; <= 0 means no
     *     limit
     * @return the log content, opening with a truncation notice when the file exceeded the cap
     * @throws IOException if the file cannot be sized
     */
    public static String read(Path path, long maxBytes) throws IOException {
        String content = FileUtils.readFileTailToStr(path, maxBytes);
        if (maxBytes <= 0) {
            return content;
        }
        // The limit that was actually applied, which is below maxBytes when a caller asked for
        // more than a single read can return.
        long limit = FileUtils.effectiveTailLimit(maxBytes);
        long size = Files.size(path);
        if (size <= limit) {
            return content;
        }
        return String.format(TRUNCATION_NOTICE_FORMAT, limit, size) + content;
    }
}
