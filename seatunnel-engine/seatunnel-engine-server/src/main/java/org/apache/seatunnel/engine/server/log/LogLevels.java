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

package org.apache.seatunnel.engine.server.log;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.LoggerConfig;

import java.util.Locale;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Parsing and applying of runtime log levels, shared by the log level endpoints. */
public final class LogLevels {

    private LogLevels() {}

    /**
     * Resolves a level name to a log4j2 {@link Level}, accepting any letter case, or returns {@code
     * null} when no such level is registered. Callers must reject {@code null} instead of handing
     * it to {@link Configurator}: a {@code null} level does not leave the logger alone, it removes
     * the explicit level so the logger falls back to its parent, and the root logger falls back to
     * {@code ERROR}. Silently lowering a level is worse than answering with an error.
     */
    public static Level parse(String name) {
        if (name == null) {
            return null;
        }
        String trimmed = name.trim();
        if (trimmed.isEmpty()) {
            return null;
        }
        Level level = Level.getLevel(trimmed);
        if (level == null) {
            level = Level.getLevel(trimmed.toUpperCase(Locale.ROOT));
        }
        return level;
    }

    /**
     * Names of all levels currently registered in log4j2, most severe first. {@link Level#values()}
     * reads a hash map, so the natural order of {@link Level} is applied to keep the message of a
     * rejected request stable.
     */
    public static String validNames() {
        return Stream.of(Level.values())
                .sorted()
                .map(Level::name)
                .collect(Collectors.joining(", "));
    }

    /** Applies a level to one logger, or to the root logger for {@link LoggerConfig#ROOT}. */
    public static void apply(String logger, Level level) {
        if (LoggerConfig.ROOT.equals(logger)) {
            Configurator.setRootLevel(level);
        } else {
            Configurator.setLevel(logger, level);
        }
    }
}
