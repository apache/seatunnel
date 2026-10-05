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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.LoggerConfig;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Parsing, applying and reverting of runtime log levels, shared by the log level endpoints. */
public final class LogLevels {

    /** The level of the logger comes from the log4j2 configuration file. */
    public static final String ORIGIN_FILE = "file";

    /** The level was set through a log level endpoint and is lost when the node restarts. */
    public static final String ORIGIN_RUNTIME_OVERRIDE = "runtime-override";

    /**
     * State every overridden logger had before its first runtime override, so that {@link
     * #reset(String)} can put it back and so that the endpoints can tell where the current level
     * comes from.
     */
    private static final Map<String, OriginalState> OVERRIDDEN_LEVELS = new ConcurrentHashMap<>();

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

    /**
     * Applies a level to one logger, or to the root logger for {@link LoggerConfig#ROOT}, and
     * returns the effective level it replaced. The level is read and replaced while holding the
     * monitor of this class, so two requests that change the same logger at the same time can not
     * both report the level that was in place before either of them ran.
     */
    public static synchronized Level apply(String logger, Level level) {
        String name = trim(logger);
        Level previousLevel = effectiveLevel(name);
        OVERRIDDEN_LEVELS.computeIfAbsent(name, OriginalState::of);
        if (LoggerConfig.ROOT.equals(name)) {
            Configurator.setRootLevel(level);
        } else {
            Configurator.setLevel(name, level);
        }
        return previousLevel;
    }

    /**
     * Reverts a logger to the state it had before its first runtime override. A logger that was not
     * configured at all loses the configuration the override added and inherits from its parent
     * again. Like {@link #apply(String, Level)} this runs under the monitor of this class, so the
     * returned levels always belong to the same revert.
     */
    public static synchronized Reverted reset(String logger) {
        String name = trim(logger);
        Level previousLevel = effectiveLevel(name);
        OriginalState original = OVERRIDDEN_LEVELS.remove(name);
        if (original == null) {
            return new Reverted(false, previousLevel, previousLevel);
        }
        if (LoggerConfig.ROOT.equals(name)) {
            // the root logger is always configured, the fallback is only defensive
            Configurator.setRootLevel(original.level == null ? Level.INFO : original.level);
        } else if (original.configured) {
            Configurator.setLevel(name, original.level);
        } else {
            LoggerContext context = LoggerContext.getContext(false);
            context.getConfiguration().removeLogger(name);
            context.updateLoggers();
        }
        return new Reverted(true, previousLevel, effectiveLevel(name));
    }

    /** Whether the current level of a logger comes from a runtime override. */
    public static boolean isOverridden(String logger) {
        return OVERRIDDEN_LEVELS.containsKey(trim(logger));
    }

    /** Either {@link #ORIGIN_FILE} or {@link #ORIGIN_RUNTIME_OVERRIDE} for one logger. */
    public static String origin(String logger) {
        return isOverridden(logger) ? ORIGIN_RUNTIME_OVERRIDE : ORIGIN_FILE;
    }

    /**
     * The level a logger was configured with before its first runtime override, {@code null} when
     * it was never overridden.
     */
    public static Level levelBeforeOverride(String logger) {
        OriginalState original = OVERRIDDEN_LEVELS.get(trim(logger));
        return original == null ? null : original.level;
    }

    /**
     * Effective level of a logger name, resolved through the closest configured ancestor, so that
     * names which are not configured themselves can be asked about as well.
     */
    public static Level effectiveLevel(String logger) {
        return configuration().getLoggerConfig(configurationName(trim(logger))).getLevel();
    }

    /**
     * Effective level of every logger of the running configuration, keyed by the name the endpoints
     * use ({@link LoggerConfig#ROOT} for the root logger), in configuration order.
     */
    public static Map<String, Level> loggers() {
        Map<String, Level> loggers = new LinkedHashMap<>();
        configuration()
                .getLoggers()
                .forEach((name, config) -> loggers.put(endpointName(name), config.getLevel()));
        return loggers;
    }

    private static Configuration configuration() {
        return LoggerContext.getContext(false).getConfiguration();
    }

    /** The root logger is named {@code root} on the endpoints and {@code ""} inside log4j2. */
    private static String configurationName(String logger) {
        return LoggerConfig.ROOT.equals(logger) ? LogManager.ROOT_LOGGER_NAME : logger;
    }

    private static String endpointName(String configurationName) {
        return LogManager.ROOT_LOGGER_NAME.equals(configurationName)
                ? LoggerConfig.ROOT
                : configurationName;
    }

    private static String trim(String logger) {
        return logger == null ? "" : logger.trim();
    }

    /**
     * Outcome of {@link #reset(String)}: whether anything was reverted, and the levels around the
     * revert.
     */
    public static final class Reverted {

        private final boolean reverted;
        private final Level previousLevel;
        private final Level level;

        private Reverted(boolean reverted, Level previousLevel, Level level) {
            this.reverted = reverted;
            this.previousLevel = previousLevel;
            this.level = level;
        }

        /** Whether the logger was overridden through an endpoint and has now been put back. */
        public boolean isReverted() {
            return reverted;
        }

        /** Effective level immediately before the revert. */
        public Level getPreviousLevel() {
            return previousLevel;
        }

        /** Effective level after the revert, the previous level when nothing was reverted. */
        public Level getLevel() {
            return level;
        }
    }

    /** What a logger looked like in the configuration before it was overridden. */
    private static final class OriginalState {

        /** Whether the logger had a configuration of its own, and not only an inherited one. */
        private final boolean configured;

        /** Level the logger was configured with. */
        private final Level level;

        private OriginalState(boolean configured, Level level) {
            this.configured = configured;
            this.level = level;
        }

        private static OriginalState of(String logger) {
            LoggerConfig config = configuration().getLoggers().get(configurationName(logger));
            return config == null
                    ? new OriginalState(false, null)
                    : new OriginalState(true, config.getLevel());
        }
    }
}
