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
     * @return mutable environment map owned by the launch context
     */
    static Map<String, String> environment(Path staging, String home) {
        String workingDirectory = ApplicationConstants.Environment.PWD.$$();
        Map<String, String> environment = new HashMap<>();
        environment.put(YarnConstants.STAGING_DIRECTORY_ENV, staging.toString());
        environment.put(YarnConstants.SEATUNNEL_HOME_ENV, workingDirectory + Path.SEPARATOR + home);
        environment.put(YarnConstants.HADOOP_CONF_DIR_ENV, workingDirectory);
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
        String classpath =
                String.format(YarnConstants.YARN_CLASSPATH, home, home, home, home, home);
        String workingDirectory = ApplicationConstants.Environment.PWD.$$();
        StringBuilder command =
                new StringBuilder("\"")
                        .append(ApplicationConstants.Environment.JAVA_HOME.$$())
                        .append("/bin/java\" -Xmx")
                        .append(
                                Math.max(
                                        YarnConstants.MINIMUM_JVM_HEAP_MB,
                                        memoryMb
                                                * (long) YarnConstants.JVM_HEAP_NUMERATOR
                                                / YarnConstants.JVM_HEAP_DENOMINATOR))
                        .append("m -Dseatunnel.home=")
                        .append("\"")
                        .append(workingDirectory)
                        .append("\"/")
                        .append(quote(home))
                        .append(" -Dhazelcast.logging.type=log4j2 -Dlog4j2.configurationFile=")
                        .append(quote(home + YarnConstants.LOG4J_CONFIG_FILE))
                        .append(" -cp ")
                        .append(quote(classpath))
                        .append(' ')
                        .append(quote(mainClass));
        for (String argument : arguments) {
            command.append(' ').append(quote(argument));
        }
        return command.append(" 1>")
                .append(quote(ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout"))
                .append(" 2>")
                .append(quote(ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"))
                .toString();
    }

    /** Quotes one untrusted argument for the NodeManager's POSIX shell command. */
    static String quote(String value) {
        return "'" + value.replace("'", "'\"'\"'") + "'";
    }
}
