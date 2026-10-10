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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Builds the environment and shell-safe Java command for a localized YARN container. */
final class YarnContainerCommand {
    private YarnContainerCommand() {}

    /**
     * Builds environment variables consumed by SeaTunnel and Hadoop inside the container.
     *
     * @param staging application-owned remote staging directory
     * @param home localized SeaTunnel home relative to the container working directory
     * @param submittingUser user propagated through {@code HADOOP_USER_NAME} for simple-auth HDFS
     * @return mutable environment map owned by the launch context
     */
    static Map<String, String> environment(Path staging, String home, String submittingUser) {
        String workingDirectory = ApplicationConstants.Environment.PWD.$$();
        Map<String, String> environment = new HashMap<>();
        environment.put(YarnConstants.STAGING_DIRECTORY_ENV, staging.toString());
        environment.put(YarnConstants.SEATUNNEL_HOME_ENV, workingDirectory + Path.SEPARATOR + home);
        environment.put(YarnConstants.HADOOP_CONF_DIR_ENV, workingDirectory);
        environment.put(YarnConstants.HADOOP_USER_NAME_ENV, submittingUser);
        return environment;
    }

    /**
     * Builds one quoted Java command with stdout and stderr redirected to the YARN log directory.
     *
     * @param home localized SeaTunnel home relative to the container working directory
     * @param memoryMb container memory used to size the JVM heap
     * @param mainClass master or worker entrypoint class
     * @param arguments entrypoint arguments, each shell quoted by this method
     * @return shell command executed by the NodeManager
     */
    static String command(String home, int memoryMb, String mainClass, List<String> arguments) {
        String classpath = classpath(home);
        String workingDirectory = ApplicationConstants.Environment.PWD.$$();
        List<String> command = new ArrayList<>();
        command.add(javaExecutable());
        command.add("-Xmx" + heapMb(memoryMb) + "m");
        command.add("-XX:+ExitOnOutOfMemoryError");
        command.add("-Dseatunnel.home=\"" + workingDirectory + "\"/" + quote(home));
        command.add("-Dhazelcast.logging.type=log4j2");
        command.add(
                "-Dlog4j2.configurationFile="
                        + quote(
                                workingDirectory
                                        + Path.SEPARATOR
                                        + home
                                        + YarnConstants.LOG4J_CONFIG_FILE));
        command.add(
                "-Dseatunnel.logs.path="
                        + quote(workingDirectory + Path.SEPARATOR + home + "/logs"));
        command.add("-Dseatunnel.logs.file_name=seatunnel-application");
        command.add("-cp");
        command.add(quote(classpath));
        command.add(quote(mainClass));
        for (String argument : arguments) {
            command.add(quote(argument));
        }
        command.add("1>" + quote(ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout"));
        command.add("2>" + quote(ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"));
        return String.join(" ", command);
    }

    private static String javaExecutable() {
        return "\"" + ApplicationConstants.Environment.JAVA_HOME.$$() + "/bin/java\"";
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
    static String quote(String value) {
        return "'" + value.replace("'", "'\"'\"'") + "'";
    }
}
