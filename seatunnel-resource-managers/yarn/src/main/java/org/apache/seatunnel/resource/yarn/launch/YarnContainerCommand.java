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

package org.apache.seatunnel.resource.yarn.launch;

import org.apache.hadoop.yarn.api.ApplicationConstants;

import java.util.ArrayList;
import java.util.List;

/** Accumulates JVM options and renders the quoted NodeManager command once all steps have run. */
public final class YarnContainerCommand {
    private final String home;
    private final String mainClass;
    private final List<String> arguments;
    private final List<String> options = new ArrayList<>();

    public YarnContainerCommand(
            String home, int memoryMb, String mainClass, List<String> arguments) {
        this.home = home;
        this.mainClass = mainClass;
        this.arguments = List.copyOf(arguments);
        options.add("\"" + ApplicationConstants.Environment.JAVA_HOME.$() + "/bin/java\"");
        options.add(YarnConstants.JVM_MAX_HEAP_PREFIX + heapMb(memoryMb) + "m");
        options.add(YarnConstants.JVM_EXIT_ON_OUT_OF_MEMORY);
        options.addAll(YarnConstants.MANDATORY_JVM_MODULE_FLAGS);
    }

    public void addOption(String option) {
        options.add(option);
    }

    public void addFileOption(String prefix, String path) {
        if (path != null) {
            options.add(prefix + containerPath(path));
        }
    }

    /**
     * Renders the completed JVM command for the NodeManager's launch script. JVM options precede
     * the main class and its arguments. Standard output and error go to the container log
     * directory.
     *
     * @return shell-escaped command for the container launch context
     */
    public String render() {
        List<String> command = new ArrayList<>(options);
        command.add("-cp");
        command.add(quote(classpath(home)));
        command.add(quote(mainClass));
        for (String argument : arguments) {
            command.add(quote(argument));
        }
        command.add("1>" + quote(ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout"));
        command.add("2>" + quote(ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"));
        // NodeManager embeds this entire command inside the double quotes of /bin/bash -c.
        return escapeDoubleQuoted(String.join(" ", command));
    }

    private static String containerPath(String path) {
        return path.startsWith("/")
                ? quote(path)
                : "\"" + ApplicationConstants.Environment.PWD.$() + "\"/" + quote(path);
    }

    /**
     * Escapes literal data for the double-quoted fields in the NodeManager's POSIX launch script.
     */
    public static String escapeDoubleQuoted(String value) {
        return value.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("$", "\\$")
                .replace("`", "\\`");
    }

    /** Builds the Java classpath from the localized YARN distribution layout. */
    private static String classpath(String home) {
        return String.format(YarnConstants.YARN_CLASSPATH, home, home, home, home, home);
    }

    /**
     * Reserves 75% of the container memory for the JVM heap: {@code max(64 MiB, memory * 3 / 4)}.
     */
    private static long heapMb(int memoryMb) {
        return Math.max(
                YarnConstants.MINIMUM_JVM_HEAP_MB,
                memoryMb
                        * (long) YarnConstants.JVM_HEAP_NUMERATOR
                        / YarnConstants.JVM_HEAP_DENOMINATOR);
    }

    /** Quotes one untrusted argument for the NodeManager's POSIX shell command. */
    public static String quote(String value) {
        return "'" + value.replace("'", "'\"'\"'") + "'";
    }
}
