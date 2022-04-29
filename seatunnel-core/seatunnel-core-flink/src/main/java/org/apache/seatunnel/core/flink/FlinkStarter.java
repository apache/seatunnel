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

package org.apache.seatunnel.core.flink;

import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.core.base.Starter;
import org.apache.seatunnel.core.flink.args.FlinkCommandArgs;

import com.beust.jcommander.JCommander;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * The SeaTunnel flink starter. This class is responsible for generate the final flink job execute command.
 */
public class FlinkStarter implements Starter {

    private static final String APP_NAME = SeatunnelFlink.class.getName();
    private static final int USAGE_EXIT_CODE = 234;
    private static final String APP_JAR_NAME = "seatunnel-core-flink.jar";
    private static final String RUN_MODE_RUN = "run";
    private static final String RUN_MODE_APPLICATION = "run-application";

    /**
     * Flink parameters, used by flink job itself. e.g. `-m yarn-cluster`
     */
    private final List<String> flinkParams = new ArrayList<>();

    /**
     * SeaTunnel parameters, used by SeaTunnel application. e.g. `-c config.conf`
     */
    private final FlinkCommandArgs flinkCommandArgs;

    /**
     * SeaTunnel flink job jar.
     */
    private final String appJar;

    FlinkStarter(String[] args) {
        this.flinkCommandArgs = parseArgs(args);
        // set the deployment mode, used to get the job jar path.
        Common.setDeployMode(flinkCommandArgs.getDeployMode().getName());
        this.appJar = Common.appLibDir().resolve(APP_JAR_NAME).toString();
    }

    @SuppressWarnings("checkstyle:RegexpSingleline")
    public static void main(String[] args) {
        FlinkStarter flinkStarter = new FlinkStarter(args);
        List<String> command = flinkStarter.buildCommands();
        String finalFLinkCommand = String.join(" ", command);
        System.out.println(finalFLinkCommand);
    }

    /**
     * Parse seatunnel args.
     *
     * @param args args
     * @return FlinkCommandArgs
     */
    private FlinkCommandArgs parseArgs(String[] args) {
        FlinkCommandArgs flinkCommandArgs = new FlinkCommandArgs();
        JCommander jCommander = JCommander.newBuilder()
            .programName("start-seatunnel-flink.sh")
            .addObject(flinkCommandArgs)
            .acceptUnknownOptions(true)
            .args(args)
            .build();
        // The args is not belongs to seatunnel, add into flink params
        flinkParams.addAll(jCommander.getUnknownOptions());
        if (flinkCommandArgs.isHelp()) {
            jCommander.usage();
            System.exit(USAGE_EXIT_CODE);
        }
        return flinkCommandArgs;
    }

    @Override
    public List<String> buildCommands() {
        List<String> command = new ArrayList<>();
        command.add("${FLINK_HOME}/bin/flink");
        command.add(flinkCommandArgs.getRunMode().getMode());
        command.addAll(flinkParams);
        command.add("-c");
        command.add(APP_NAME);
        command.add(appJar);
        command.add("--config");
        command.add(flinkCommandArgs.getConfigFile());
        if (flinkCommandArgs.isCheckConfig()) {
            command.add("--check");
        }
        // set System properties
        flinkCommandArgs.getVariables().stream()
            .filter(Objects::nonNull)
            .map(String::trim)
            .forEach(variable -> command.add("-D" + variable));
        return command;
    }

}
