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

package org.apache.seatunnel.e2e.connector.sentry;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.core.starter.utils.ConfigBuilder;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.impl.Log4jContextFactory;
import org.apache.logging.log4j.core.layout.PatternLayout;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SentryMaskingTest {
    @Test
    void startupLogsMaskTokenWithoutChangingExecutableConfiguration() {
        List<String> messages = new ArrayList<>();
        AbstractAppender appender =
                new AbstractAppender(
                        "sentry-config-test",
                        null,
                        PatternLayout.createDefaultLayout(),
                        true,
                        Property.EMPTY_ARRAY) {
                    @Override
                    public void append(LogEvent event) {
                        messages.add(event.getMessage().getFormattedMessage());
                    }
                };
        // Initialize the actual SLF4J logging context before attaching the capture. Java 8 and 11
        // can select different caller-classloader contexts in the shaded test classpath.
        ConfigBuilder.of(Collections.emptyMap());
        Set<LoggerContext> contexts =
                new HashSet<>(
                        ((Log4jContextFactory) LogManager.getFactory())
                                .getSelector()
                                .getLoggerContexts());
        contexts.add((LoggerContext) LogManager.getContext(false));
        Map<Logger, Level> loggers = new HashMap<>();
        appender.start();
        for (LoggerContext context : contexts) {
            Logger logger = context.getLogger(ConfigBuilder.class.getName());
            loggers.put(logger, logger.getLevel());
            logger.addAppender(appender);
            logger.setLevel(Level.INFO);
        }
        try {
            Map<String, Object> source = Collections.singletonMap("token", "DO-NOT-LOG-SENTRY");
            Config config =
                    ConfigBuilder.of(
                            Collections.singletonMap("source", Collections.singletonList(source)));
            assertEquals(
                    "DO-NOT-LOG-SENTRY", config.getConfigList("source").get(0).getString("token"));
            assertTrue(
                    messages.stream().anyMatch(message -> message.contains("Parsed config file")));
            assertFalse(messages.toString().contains("DO-NOT-LOG-SENTRY"));
            assertTrue(messages.toString().contains("******"));
        } finally {
            loggers.forEach(
                    (logger, level) -> {
                        logger.removeAppender(appender);
                        logger.setLevel(level);
                    });
            appender.stop();
        }
    }
}
