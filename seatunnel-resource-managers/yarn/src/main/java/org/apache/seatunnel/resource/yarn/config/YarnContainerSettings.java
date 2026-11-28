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

package org.apache.seatunnel.resource.yarn.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.resource.yarn.launch.YarnConstants;

import org.apache.hadoop.yarn.api.ApplicationConstants;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** The launch options carried to both container roles in the private localization manifest. */
public final class YarnContainerSettings {
    private static final List<Option<String>> FILE_OPTIONS =
            List.of(
                    YarnOptions.JVM_OPTIONS,
                    YarnOptions.MASTER_JVM_OPTIONS,
                    YarnOptions.WORKER_JVM_OPTIONS,
                    YarnOptions.MASTER_HAZELCAST_CONFIG,
                    YarnOptions.WORKER_HAZELCAST_CONFIG,
                    YarnOptions.LOG4J_CONFIG_FILE,
                    YarnOptions.SEATUNNEL_CONFIG_FILE);
    private static final Pattern ENV_NAME = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");
    private static final Set<String> RESERVED_ENV =
            Set.of(
                    YarnConstants.SEATUNNEL_HOME_ENV,
                    YarnConstants.STAGING_DIRECTORY_ENV,
                    YarnConstants.HADOOP_CONF_DIR_ENV,
                    YarnConstants.HADOOP_USER_NAME_ENV,
                    ApplicationConstants.CONTAINER_TOKEN_FILE_ENV_NAME);

    private final Map<String, String> environment = new HashMap<>();
    private final Set<String> shippedFiles;
    private final Map<String, String> configuredPaths = new HashMap<>();

    YarnContainerSettings(ReadonlyConfig options, Set<String> shippedFiles) {
        this.shippedFiles = Set.copyOf(shippedFiles);
        for (String key : options.getSourceMap().keySet()) {
            if (key.startsWith(YarnConstants.ENV_PREFIX)) {
                String name = key.substring(YarnConstants.ENV_PREFIX.length());
                String value = options.get(YarnOptions.environmentVariable(name));
                if (!ENV_NAME.matcher(name).matches() || RESERVED_ENV.contains(name)) {
                    throw new IllegalArgumentException(
                            "Invalid or reserved YARN container environment variable: " + key);
                }
                if (value == null || unsafeLiteral(value)) {
                    throw new IllegalArgumentException(
                            key + " contains a control character or a YARN expansion marker");
                }
                environment.put(name, value);
            }
        }
        configuredPaths.put(
                YarnOptions.LOG4J_CONFIG_FILE.key(), YarnOptions.LOG4J_CONFIG_FILE.defaultValue());
        for (Option<String> option : FILE_OPTIONS) {
            options.getOptional(option)
                    .ifPresent(
                            value ->
                                    configuredPaths.put(option.key(), validatePath(option, value)));
        }
    }

    private static boolean unsafeLiteral(String value) {
        return value.chars().anyMatch(Character::isISOControl)
                || value.contains(ApplicationConstants.PARAMETER_EXPANSION_LEFT)
                || value.contains(ApplicationConstants.PARAMETER_EXPANSION_RIGHT)
                || value.contains(ApplicationConstants.LOG_DIR_EXPANSION_VAR)
                || value.contains(ApplicationConstants.CLASS_PATH_SEPARATOR);
    }

    private String validatePath(Option<String> option, String raw) {
        String value = raw.trim();
        String[] components = value.split("/", -1);
        boolean shipped = value.startsWith(YarnConstants.SHIPPED_FILES_DIRECTORY + "/");
        if (value.isEmpty()
                || value.endsWith("/")
                || value.contains(":")
                || value.contains("\\")
                || unsafeLiteral(value)
                || Arrays.asList(components).contains("..")
                || Arrays.asList(components).contains(".")
                || (shipped && !shippedFiles.contains(value))
                || (!shipped
                        && !value.startsWith("/")
                        && components.length > 1
                        && option != YarnOptions.SEATUNNEL_CONFIG_FILE)) {
            throw new IllegalArgumentException(
                    option.key()
                            + " must reference a container file, a distribution file or a declared files/<alias>");
        }
        String lower = value.toLowerCase(Locale.ROOT);
        if ((option == YarnOptions.SEATUNNEL_CONFIG_FILE && !lower.matches(".*\\.ya?ml"))
                || ((option == YarnOptions.MASTER_HAZELCAST_CONFIG
                                || option == YarnOptions.WORKER_HAZELCAST_CONFIG)
                        && !lower.matches(".*\\.(ya?ml|xml)"))
                || (option == YarnOptions.LOG4J_CONFIG_FILE
                        && !lower.matches(".*\\.(properties|xml|ya?ml|json|jsn)"))) {
            throw new IllegalArgumentException(
                    option.key() + " has an unsupported configuration file extension: " + value);
        }
        return value;
    }

    public Map<String, String> getEnvironment() {
        return Collections.unmodifiableMap(environment);
    }

    public Set<String> getShippedFiles() {
        return shippedFiles;
    }

    /** Resolves paths against the container working directory or the relocated distribution. */
    public String path(Option<String> option, String home) {
        String value = configuredPaths.get(option.key());
        if (value == null || YarnConstants.DISABLED.equalsIgnoreCase(value)) {
            return null;
        }
        if (value.startsWith("/")
                || value.startsWith(YarnConstants.SHIPPED_FILES_DIRECTORY + "/")) {
            return value;
        }
        return home
                + (option == YarnOptions.SEATUNNEL_CONFIG_FILE
                        ? ""
                        : YarnConstants.CONFIG_DIRECTORY)
                + value;
    }

    /**
     * Records launch settings for the AM and workers. An optional default is included when found in
     * the archive, and an explicit file option takes precedence.
     *
     * @param manifest distribution manifest to populate
     * @param distributionFileExists tests paths relative to the distribution home
     */
    public void writeTo(Properties manifest, Predicate<String> distributionFileExists) {
        // A minimal distribution may omit the standard argument files or use the built-in engine
        // configuration.
        for (Option<String> option :
                List.of(
                        YarnOptions.JVM_OPTIONS,
                        YarnOptions.MASTER_JVM_OPTIONS,
                        YarnOptions.WORKER_JVM_OPTIONS,
                        YarnOptions.SEATUNNEL_CONFIG_FILE)) {
            String value = option.defaultValue();
            String relative =
                    option == YarnOptions.SEATUNNEL_CONFIG_FILE
                            ? value
                            : YarnConstants.CONFIG_DIRECTORY + value;
            if (distributionFileExists.test(relative)) {
                manifest.setProperty(option.key(), value);
            }
        }
        configuredPaths.forEach(manifest::setProperty);
        environment.forEach(
                (key, value) -> manifest.setProperty(YarnConstants.ENV_PREFIX + key, value));
        shippedFiles.forEach(
                path -> {
                    String alias =
                            path.substring(YarnConstants.SHIPPED_FILES_DIRECTORY.length() + 1);
                    manifest.setProperty(YarnConstants.SHIP_FILES_PREFIX + alias, alias);
                });
    }

    /** Rebuilds runtime settings and localized paths from the manifest written during staging. */
    public static YarnContainerSettings read(Properties manifest) {
        Map<String, Object> options = new HashMap<>();
        manifest.stringPropertyNames().forEach(key -> options.put(key, manifest.getProperty(key)));
        Set<String> files =
                manifest.stringPropertyNames().stream()
                        .filter(key -> key.startsWith(YarnConstants.SHIP_FILES_PREFIX))
                        .map(
                                key ->
                                        YarnConstants.SHIPPED_FILES_DIRECTORY
                                                + "/"
                                                + key.substring(
                                                        YarnConstants.SHIP_FILES_PREFIX.length()))
                        .collect(Collectors.toSet());
        return new YarnContainerSettings(ReadonlyConfig.fromMap(options), files);
    }
}
