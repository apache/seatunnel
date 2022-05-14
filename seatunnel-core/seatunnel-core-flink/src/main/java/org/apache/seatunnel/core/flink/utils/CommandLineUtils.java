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

package org.apache.seatunnel.core.flink.utils;

import static org.apache.seatunnel.core.flink.constant.FlinkConstant.USAGE_EXIT_CODE;

import org.apache.seatunnel.core.flink.args.FlinkCommandArgs;
import org.apache.seatunnel.core.flink.config.FlinkJobType;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.UnixStyleUsageFormatter;

import java.util.ArrayList;
import java.util.List;

public class CommandLineUtils {

    private CommandLineUtils() {
        throw new UnsupportedOperationException("CommandLineUtils is a utility class and cannot be instantiated");
    }

    public static FlinkCommandArgs parseCommandArgs(String[] args, FlinkJobType jobType) {
        FlinkCommandArgs flinkCommandArgs = new FlinkCommandArgs();
        JCommander jCommander = JCommander.newBuilder()
            .programName(jobType.getType())
            .addObject(flinkCommandArgs)
            .acceptUnknownOptions(true)
            .args(args)
            .build();
        // The args is not belongs to seatunnel, add into flink params
        flinkCommandArgs.setFlinkParams(jCommander.getUnknownOptions());
        if (flinkCommandArgs.isHelp()) {
            jCommander.setUsageFormatter(new UnixStyleUsageFormatter(jCommander));
            jCommander.usage();
            System.exit(USAGE_EXIT_CODE);
        }
        return flinkCommandArgs;

    }

    public static List<String> buildFlinkCommand(FlinkCommandArgs flinkCommandArgs, String className, String jarPath) {
        List<String> command = new ArrayList<>();
        command.add("${FLINK_HOME}/bin/flink");
        command.add(flinkCommandArgs.getRunMode().getMode());
        command.addAll(flinkCommandArgs.getFlinkParams());
        command.add("-c");
        command.add(className);
        command.add(jarPath);
        command.add("--config");
        command.add(flinkCommandArgs.getConfigFile());
        if (flinkCommandArgs.isCheckConfig()) {
            command.add("--check");
        }

        return command;
    }
}
