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

package org.apache.seatunnel.edge.agent.connector.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import lombok.Getter;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.UnsupportedCharsetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

@Getter
public class FileCollectConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String id;
    private final List<String> paths;
    private final String encoding;
    private final boolean readFromBeginning;
    private final String multilinePattern;
    private final String multilineMatch;
    private final boolean multilineNegate;
    private final int multilineMaxLines;
    private final long multilineFlushIdleTimeoutMs;
    private final String outputType;
    private final Charset charset;
    private final String onError;
    private final long globScanIntervalMs;
    private final long closeInactiveMs;

    public FileCollectConfig(ReadonlyConfig config) {
        Objects.requireNonNull(config, "config");
        this.id = config.get(FileCollectOptions.ID);
        if (this.id == null || this.id.trim().isEmpty()) {
            throw new IllegalArgumentException("input.id must be non-empty.");
        }
        List<String> pathList = config.get(FileCollectOptions.PATHS);
        if (pathList == null || pathList.isEmpty()) {
            throw new IllegalArgumentException(
                    "input.paths must not be empty (input id=" + this.id + ").");
        }
        for (String path : pathList) {
            if (path == null || path.trim().isEmpty()) {
                throw new IllegalArgumentException(
                        "input.paths must not contain blank entries (input id=" + this.id + ").");
            }
        }
        this.paths = Collections.unmodifiableList(new ArrayList<>(pathList));
        this.encoding = config.get(FileCollectOptions.ENCODING);
        this.charset = resolveCharset(this.encoding, this.id);
        this.readFromBeginning = config.get(FileCollectOptions.READ_FROM_BEGINNING);
        this.multilinePattern =
                config.getOptional(FileCollectOptions.MULTILINE_PATTERN).orElse(null);
        this.multilineMatch = config.get(FileCollectOptions.MULTILINE_MATCH);
        validateMultilineMatch(this.multilineMatch);
        this.multilineNegate = config.get(FileCollectOptions.MULTILINE_NEGATE);
        this.multilineMaxLines = config.get(FileCollectOptions.MULTILINE_MAX_LINES);
        if (this.multilineMaxLines < 1) {
            throw new IllegalArgumentException(
                    "input.multiline.max-lines must be >= 1 when set (input id=" + this.id + ").");
        }
        this.multilineFlushIdleTimeoutMs =
                config.get(FileCollectOptions.MULTILINE_FLUSH_IDLE_TIMEOUT_MS);
        if (this.multilineFlushIdleTimeoutMs < 0L) {
            throw new IllegalArgumentException(
                    "input.multiline.flush-idle-timeout-ms must not be negative (input id="
                            + this.id
                            + ").");
        }
        if (isMultilineEnabled() && this.multilineFlushIdleTimeoutMs <= 0L) {
            throw new IllegalArgumentException(
                    "input.multiline.flush-idle-timeout-ms must be > 0 when multiline is enabled,"
                            + " otherwise the buffer may never flush (input id="
                            + this.id
                            + ").");
        }
        this.outputType = config.get(FileCollectOptions.OUTPUT_FORMAT_TYPE);
        validateOutputType(this.outputType);
        this.onError = config.get(FileCollectOptions.ON_ERROR);
        validateOnError(this.onError);
        this.globScanIntervalMs = config.get(FileCollectOptions.GLOB_SCAN_INTERVAL_MS);
        if (this.globScanIntervalMs < 1L) {
            throw new IllegalArgumentException(
                    "input.glob-scan-interval-ms must be >= 1 when set (input id="
                            + this.id
                            + ").");
        }
        this.closeInactiveMs = config.get(FileCollectOptions.CLOSE_INACTIVE_MS);
        if (this.closeInactiveMs < 0L) {
            throw new IllegalArgumentException(
                    "input.close-inactive-ms must be >= 0 when set (input id=" + this.id + ").");
        }
    }

    public static FileCollectConfig from(ReadonlyConfig config) {
        return new FileCollectConfig(config);
    }

    public boolean isMultilineEnabled() {
        return multilinePattern != null && !multilinePattern.isEmpty();
    }

    public boolean isJsonOutput() {
        return "json".equalsIgnoreCase(outputType);
    }

    public boolean isSkipOnError() {
        return !"fail".equalsIgnoreCase(onError);
    }

    private static void validateOnError(String value) {
        if (!value.equalsIgnoreCase("skip") && !value.equalsIgnoreCase("fail")) {
            throw new IllegalArgumentException("input.on-error must be \"skip\" or \"fail\".");
        }
    }

    private static void validateMultilineMatch(String value) {
        if (!value.equalsIgnoreCase("after") && !value.equalsIgnoreCase("before")) {
            throw new IllegalArgumentException(
                    "input.multiline.match must be \"after\" or \"before\".");
        }
    }

    private static void validateOutputType(String value) {
        if (!value.equalsIgnoreCase("line") && !value.equalsIgnoreCase("json")) {
            throw new IllegalArgumentException(
                    "input.output-format.type must be \"line\" or \"json\".");
        }
    }

    private static Charset resolveCharset(String encodingName, String inputId) {
        try {
            return Charset.forName(encodingName);
        } catch (IllegalCharsetNameException | UnsupportedCharsetException e) {
            throw new IllegalArgumentException(
                    "input.encoding is not supported: "
                            + encodingName
                            + " (input id="
                            + inputId
                            + ")",
                    e);
        }
    }
}
