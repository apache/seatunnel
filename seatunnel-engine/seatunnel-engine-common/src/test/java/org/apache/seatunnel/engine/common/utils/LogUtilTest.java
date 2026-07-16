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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;

import org.junit.jupiter.api.Test;

import java.net.URL;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class LogUtilTest {

    @Test
    void shouldGetLogPathFromFileAppender() throws Exception {
        LoggerContext context = (LoggerContext) LogManager.getContext(false);
        Configuration originalConfiguration = context.getConfiguration();
        URL configuration = getClass().getClassLoader().getResource("log4j2-file-only.properties");
        assertNotNull(configuration);

        try {
            context.setConfigLocation(configuration.toURI());
            assertEquals("target/logs", LogUtil.getLogPath());
        } finally {
            context.setConfiguration(originalConfiguration);
        }
    }
}
