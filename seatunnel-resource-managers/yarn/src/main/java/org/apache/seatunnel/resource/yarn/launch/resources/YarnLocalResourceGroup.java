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

package org.apache.seatunnel.resource.yarn.launch.resources;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.resource.yarn.config.YarnOptions;
import org.apache.seatunnel.resource.yarn.launch.YarnConstants;

import org.apache.hadoop.fs.Path;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;

/** A named set of files localized from a source directory or from literal deployment content. */
public final class YarnLocalResourceGroup {
    private static final Pattern GROUP_NAME = Pattern.compile("[A-Za-z0-9][A-Za-z0-9_-]*");
    private static final String FILE_NAME = "[A-Za-z0-9][A-Za-z0-9._-]*";
    private static final Pattern FILE = Pattern.compile(FILE_NAME);
    private static final String DIRECTORY = YarnConstants.SHIPPED_FILES_DIRECTORY + "/";
    private static final Pattern TARGET =
            Pattern.compile(DIRECTORY + FILE_NAME + "(?:/" + FILE_NAME + ")*");

    private final String name;
    private final Map<String, String> content = new LinkedHashMap<>();
    private final Map<String, String> destinations = new LinkedHashMap<>();
    private Path sourceDirectory;

    private YarnLocalResourceGroup(String name) {
        this.name = name;
    }

    /**
     * Resolves grouped and individual file declarations before staging. The submission code uses
     * the container-relative path to pick out an AM-only job file and to localize the remaining
     * files for both roles.
     *
     * @param options deployment options containing file sources, inline contents and destinations
     * @return file sources keyed by their paths under {@code files/}
     * @throws IllegalArgumentException for invalid declarations or conflicting destination paths
     */
    public static Map<String, YarnFileSource> resolve(ReadonlyConfig options) {
        Map<String, YarnFileSource> files = new LinkedHashMap<>();
        Map<String, YarnLocalResourceGroup> groups = new LinkedHashMap<>();
        for (String key : options.getSourceMap().keySet()) {
            if (key.startsWith(YarnConstants.LOCAL_RESOURCES_PREFIX)) {
                String entry = key.substring(YarnConstants.LOCAL_RESOURCES_PREFIX.length());
                int separator = entry.indexOf('.');
                if (separator < 1 || !GROUP_NAME.matcher(entry.substring(0, separator)).matches()) {
                    throw new IllegalArgumentException("Invalid YARN local resource group: " + key);
                }
                groups.computeIfAbsent(entry.substring(0, separator), YarnLocalResourceGroup::new)
                        .read(entry.substring(separator + 1), options);
            } else if (key.startsWith(YarnConstants.SHIP_FILES_PREFIX)) {
                String alias = fileName(key, YarnConstants.SHIP_FILES_PREFIX);
                Option<String> option = YarnOptions.shipFile(alias);
                put(
                        files,
                        DIRECTORY + alias,
                        YarnFileSource.fromUri(
                                YarnFileSource.sourceUri(required(options, option), key)));
            } else if (key.startsWith(YarnConstants.INLINE_FILES_PREFIX)) {
                String alias = fileName(key, YarnConstants.INLINE_FILES_PREFIX);
                put(
                        files,
                        DIRECTORY + alias,
                        YarnFileSource.inline(options.get(YarnOptions.inlineFile(alias))));
            }
        }
        groups.values().forEach(group -> group.addFiles(files));
        for (String path : files.keySet()) {
            for (int end = path.lastIndexOf('/'); end > 0; end = path.lastIndexOf('/', end - 1)) {
                if (files.containsKey(path.substring(0, end))) {
                    throw new IllegalArgumentException(
                            "A YARN local resource is also a parent directory of " + path);
                }
            }
        }
        return files;
    }

    private void read(String field, ReadonlyConfig options) {
        String prefix = YarnConstants.LOCAL_RESOURCES_PREFIX + name + ".";
        String key = prefix + field;
        if (field.equals(YarnConstants.SOURCE_DIR)) {
            String directory = required(options, YarnOptions.resourceDirectory(name));
            sourceDirectory = new Path(YarnFileSource.sourceUri(directory, key));
        } else if (field.startsWith(YarnConstants.CONTENT_PREFIX)) {
            String file = fileName(key, prefix + YarnConstants.CONTENT_PREFIX);
            content.put(file, options.get(YarnOptions.resourceContent(name, file)));
        } else if (field.startsWith(YarnConstants.LOCALIZE_PREFIX)) {
            String file = fileName(key, prefix + YarnConstants.LOCALIZE_PREFIX);
            String target = required(options, YarnOptions.resourceTarget(name, file));
            if (!TARGET.matcher(target).matches()) {
                throw new IllegalArgumentException(
                        key + " requires a relative file path under " + DIRECTORY);
            }
            destinations.put(file, target);
        } else {
            throw new IllegalArgumentException("Unknown YARN local resource option: " + key);
        }
    }

    private void addFiles(Map<String, YarnFileSource> files) {
        String prefix = YarnConstants.LOCAL_RESOURCES_PREFIX + name;
        if (sourceDirectory != null) {
            if (!content.isEmpty() || destinations.isEmpty()) {
                throw new IllegalArgumentException(
                        prefix
                                + " with source-dir requires localize entries and cannot also declare content");
            }
            destinations.forEach(
                    (file, target) ->
                            put(
                                    files,
                                    target,
                                    YarnFileSource.fromUri(
                                            new Path(sourceDirectory, file).toUri())));
        } else {
            if (!content.keySet().containsAll(destinations.keySet())) {
                throw new IllegalArgumentException(
                        prefix + " must supply content or a source-dir for every localized file");
            }
            content.forEach(
                    (file, value) ->
                            put(
                                    files,
                                    destinations.getOrDefault(file, DIRECTORY + file),
                                    YarnFileSource.inline(value)));
        }
    }

    private static void put(
            Map<String, YarnFileSource> files, String target, YarnFileSource source) {
        if (files.putIfAbsent(target, source) != null) {
            throw new IllegalArgumentException("Multiple YARN local resources target " + target);
        }
    }

    private static String fileName(String key, String prefix) {
        String file = key.substring(prefix.length());
        if (!FILE.matcher(file).matches()) {
            throw new IllegalArgumentException(key + " requires a single file name");
        }
        return file;
    }

    private static String required(ReadonlyConfig options, Option<String> option) {
        String value = options.get(option);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(option.key() + " requires a path");
        }
        return value.trim();
    }
}
