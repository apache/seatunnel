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

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

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
        return getLogPath(getLogConfiguration());
    }

    static String getLogPath(PropertiesConfiguration config)
            throws NoSuchFieldException, IllegalAccessException {
        String routingAppender = "routingAppender";
        String fileAppender = "fileAppender";
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
            return getParentLogPath(routingLogFilePath, routingAppender);
        } else if (logRef.equals(fileAppender)) {
            return getParentLogPath(fileLogPath, fileAppender);
        } else {
            throw new IllegalArgumentException(
                    String.format("Log file path is empty, get logRef : %s", logRef));
        }
    }

    static String getParentLogPath(String logFilePath, String appenderName) {
        if (StringUtils.isBlank(logFilePath)) {
            throw new IllegalArgumentException(
                    String.format("Log file path is empty for appender: %s", appenderName));
        }
        int separatorIndex = Math.max(logFilePath.lastIndexOf('/'), logFilePath.lastIndexOf('\\'));
        if (separatorIndex < 0) {
            return ".";
        }
        if (separatorIndex == 0) {
            return logFilePath.substring(0, 1);
        }
        if (separatorIndex == 2
                && logFilePath.length() > 2
                && Character.isLetter(logFilePath.charAt(0))
                && logFilePath.charAt(1) == ':') {
            return logFilePath.substring(0, 2) + preferredSeparator(logFilePath);
        }
        return normalizeParentSeparators(logFilePath.substring(0, separatorIndex));
    }

    private static String normalizeParentSeparators(String path) {
        char separator = preferredSeparator(path);
        StringBuilder normalized = new StringBuilder(path.length());
        for (int index = 0; index < path.length(); index++) {
            char current = path.charAt(index);
            if (current == '/' || current == '\\') {
                current = separator;
            }
            boolean duplicateSeparator =
                    current == separator
                            && normalized.length() > 0
                            && normalized.charAt(normalized.length() - 1) == separator;
            if (duplicateSeparator && index != 1) {
                continue;
            }
            normalized.append(current);
        }
        return normalized.toString();
    }

    private static char preferredSeparator(String path) {
        if (path.startsWith("\\\\")
                || (path.length() > 1
                        && Character.isLetter(path.charAt(0))
                        && path.charAt(1) == ':')
                || path.indexOf('\\') >= 0) {
            return '\\';
        }
        return '/';
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
