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

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.builder.api.Component;
import org.apache.logging.log4j.core.config.builder.impl.BuiltConfiguration;
import org.apache.logging.log4j.core.config.properties.PropertiesConfiguration;
import org.apache.logging.log4j.core.lookup.StrSubstitutor;

import java.lang.reflect.Field;

public class LogUtil {

    /** Get configuration log path by log4j */
    public static String getLogPath() throws NoSuchFieldException, IllegalAccessException {
        String routingAppender = "routingAppender";
        String fileAppender = "fileAppender";
        PropertiesConfiguration config = getLogConfiguration();
        // Get routingAppender log file path
        String routingLogFilePath = getRoutingLogFilePath(config);

        // Get fileAppender log file path
        String fileLogPath = getFileLogPath(config);
        String logRef =
                config.getLoggerConfig(StringUtils.EMPTY).getAppenderRefs().stream()
                        .map(Object::toString)
                        .filter(ref -> ref.contains(routingAppender) || ref.contains(fileAppender))
                        .findFirst()
                        .orElse(StringUtils.EMPTY);
        if (logRef.equals(routingAppender)) {
            return routingLogFilePath.substring(0, routingLogFilePath.lastIndexOf("/"));
        } else if (logRef.equals(fileAppender)) {
            return fileLogPath.substring(0, routingLogFilePath.lastIndexOf("/"));
        } else {
            throw new IllegalArgumentException(
                    String.format("Log file path is empty, get logRef : %s", logRef));
        }
    }

    private static PropertiesConfiguration getLogConfiguration() {
        LoggerContext context = (LoggerContext) LogManager.getContext(false);
        return (PropertiesConfiguration) context.getConfiguration();
    }

    private static String getRoutingLogFilePath(PropertiesConfiguration config)
            throws NoSuchFieldException, IllegalAccessException {
        Field propertiesField = BuiltConfiguration.class.getDeclaredField("appendersComponent");
        propertiesField.setAccessible(true);
        Component propertiesComponent = (Component) propertiesField.get(config);
        StrSubstitutor substitutor = config.getStrSubstitutor();
        return propertiesComponent.getComponents().stream()
                .filter(
                        component ->
                                "routingAppender".equals(component.getAttributes().get("name")))
                .flatMap(component -> component.getComponents().stream())
                .flatMap(component -> component.getComponents().stream())
                .flatMap(component -> component.getComponents().stream())
                .map(component -> substitutor.replace(component.getAttributes().get("fileName")))
                .findFirst()
                .orElse(null);
    }

    private static String getFileLogPath(PropertiesConfiguration config)
            throws NoSuchFieldException, IllegalAccessException {
        Field propertiesField = BuiltConfiguration.class.getDeclaredField("appendersComponent");
        propertiesField.setAccessible(true);
        Component propertiesComponent = (Component) propertiesField.get(config);
        StrSubstitutor substitutor = config.getStrSubstitutor();
        return propertiesComponent.getComponents().stream()
                .filter(component -> "fileAppender".equals(component.getAttributes().get("name")))
                .map(component -> substitutor.replace(component.getAttributes().get("fileName")))
                .findFirst()
                .orElse(null);
    }
}
