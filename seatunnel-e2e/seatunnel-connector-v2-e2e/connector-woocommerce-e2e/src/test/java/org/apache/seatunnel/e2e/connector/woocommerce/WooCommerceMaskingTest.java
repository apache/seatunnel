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

package org.apache.seatunnel.e2e.connector.woocommerce;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.multitable.MultiTableFailureHelper;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.woocommerce.source.WooCommerceSource;
import org.apache.seatunnel.connectors.seatunnel.woocommerce.source.WooCommerceSourceFactory;
import org.apache.seatunnel.core.starter.utils.ConfigBuilder;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
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

class WooCommerceMaskingTest {
    @Test
    void actualConfigLoadLogsNeitherConsumerCredential() throws Exception {
        // Initialize the SLF4J binding before attaching the test appender.
        ConfigBuilder.of(Collections.emptyMap());
        Set<LoggerContext> contexts =
                new HashSet<>(
                        ((Log4jContextFactory) LogManager.getFactory())
                                .getSelector()
                                .getLoggerContexts());
        contexts.add((LoggerContext) LogManager.getContext(false));
        Map<Logger, Level> loggers = new HashMap<>();
        List<String> messages = new ArrayList<>();
        AbstractAppender capture =
                new AbstractAppender(
                        "woocommerce-mask-test", null, PatternLayout.createDefaultLayout(), false) {
                    @Override
                    public void append(LogEvent event) {
                        messages.add(event.getMessage().getFormattedMessage());
                    }
                };
        capture.start();
        for (LoggerContext context : contexts) {
            Logger logger = context.getLogger(ConfigBuilder.class.getName());
            loggers.put(logger, logger.getLevel());
            logger.addAppender(capture);
            logger.setLevel(Level.INFO);
        }
        try {
            Config config =
                    ConfigBuilder.of(
                            getClass().getResource("/woocommerce_to_assert.conf").getPath());
            String logs = String.join("\n", messages);
            assertTrue(logs.contains("Parsed config"));
            assertTrue(logs.contains("******"));
            assertFalse(logs.contains("ck_0000000000000000000000000000000000000001"));
            assertFalse(logs.contains("cs_0000000000000000000000000000000000000002"));
            assertEquals(
                    "ck_0000000000000000000000000000000000000001",
                    config.getConfigList("source").get(0).getString("consumer_key"));
            ReadonlyConfig sourceOptions =
                    MultiTableFailureHelper.withMultiTableFailurePolicy(
                            ReadonlyConfig.fromConfig(config.getConfigList("source").get(0)),
                            ReadonlyConfig.fromConfig(config.getConfig("env")));
            WooCommerceSourceFactory factory = new WooCommerceSourceFactory();
            ConfigValidator.of(sourceOptions).validate(factory.optionRule());
            Object source =
                    factory.createSource(
                                    new TableSourceFactoryContext(
                                            sourceOptions, getClass().getClassLoader()))
                            .createSource();
            assertTrue(source instanceof WooCommerceSource);
        } finally {
            loggers.forEach(
                    (logger, level) -> {
                        logger.removeAppender(capture);
                        logger.setLevel(level);
                    });
            capture.stop();
        }
    }
}
