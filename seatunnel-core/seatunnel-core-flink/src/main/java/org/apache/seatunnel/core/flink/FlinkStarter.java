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
import org.apache.seatunnel.core.base.utils.CommandLineUtils;
import org.apache.seatunnel.core.flink.args.FlinkCommandArgs;
import org.apache.seatunnel.core.flink.config.FlinkJobType;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * The SeaTunnel flink starter. This class is responsible for generate the final flink job execute command.
 */
public class FlinkStarter implements Starter {

    private static final String APP_NAME = SeatunnelFlink.class.getName();
    private static final String APP_JAR_NAME = "seatunnel-core-flink.jar";

    /**
     * SeaTunnel parameters, used by SeaTunnel application. e.g. `-c config.conf`
     */
    private final FlinkCommandArgs flinkCommandArgs;

    /**
     * SeaTunnel flink job jar.
     */
    private final String appJar;

    FlinkStarter(String[] args) {
        this.flinkCommandArgs = CommandLineUtils.parse(args, new FlinkCommandArgs(), FlinkJobType.JAR.getType(), true);
        flinkCommandArgs.getVariables()
                .stream()
                .filter(Objects::nonNull)
                .map(variable -> variable.split("=", 2))
                .filter(pair -> pair.length == 2)
                .forEach(pair -> System.setProperty(pair[0], pair[1]));
        // set the deployment mode, used to get the job jar path.
        Common.setDeployMode(flinkCommandArgs.getDeployMode());
        Common.setStarter(true);
        this.appJar = Common.appLibDir().resolve(APP_JAR_NAME).toString();
    }

    @SuppressWarnings("checkstyle:RegexpSingleline")
    public static void main(String[] args) {
        FlinkStarter flinkStarter = new FlinkStarter(args);
        System.out.println(String.join(" ", flinkStarter.buildCommands()));
    }

    @Override
    public List<String> buildCommands() {
        List<String> command = new ArrayList<>();
        command.add("${FLINK_HOME}/bin/flink");
        command.add(flinkCommandArgs.getRunMode().getMode());
        command.addAll(flinkCommandArgs.getOriginalParameters());
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
