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

package org.apache.seatunnel.engine.common.utils;

import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.properties.PropertiesConfiguration;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.nio.file.Paths;

public class LogUtilTest {

    @Test
    void shouldGetLogPathFromFileAppender() throws Exception {
        String expected =
                Paths.get(System.getProperty("java.io.tmpdir"), "seatunnel-logutil-test")
                        .toString();
        assertLogPath("log4j2-file-only.properties", expected);
    }

    @Test
    void shouldGetLogPathFromRoutingAppender() throws Exception {
        assertLogPath("log4j2-routing-only.properties", "target/routing-logs");
    }

    @Test
    void shouldUseCurrentDirectoryForPathlessFileAppender() {
        Assertions.assertEquals(".", LogUtil.getParentLogPath("seatunnel.log", "fileAppender"));
    }

    @Test
    void shouldResolveWindowsStyleLogPath() {
        Assertions.assertEquals(
                "C:\\seatunnel\\logs",
                LogUtil.getParentLogPath(
                        "C:\\seatunnel\\logs\\job-${ctx:ST-JID}.log", "routingAppender"));
    }

    @Test
    void shouldNormalizeMixedWindowsSeparators() {
        Assertions.assertEquals(
                "C:\\seatunnel\\logs",
                LogUtil.getParentLogPath(
                        "C:\\seatunnel\\logs/job-${ctx:ST-JID}.log", "fileAppender"));
    }

    @Test
    void shouldRejectEmptyLogPath() {
        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> LogUtil.getParentLogPath(null, "fileAppender"));
        Assertions.assertEquals(
                "Log file path is empty for appender: fileAppender", exception.getMessage());
    }

    private void assertLogPath(String resource, String expected) throws Exception {
        URL configuration = getClass().getClassLoader().getResource(resource);
        Assertions.assertNotNull(configuration);
        LoggerContext context = new LoggerContext("LogUtilTest-" + resource);

        try {
            context.setConfigLocation(configuration.toURI());
            Assertions.assertInstanceOf(PropertiesConfiguration.class, context.getConfiguration());
            Assertions.assertEquals(
                    expected,
                    LogUtil.getLogPath((PropertiesConfiguration) context.getConfiguration()));
        } finally {
            context.stop();
        }
    }
}
