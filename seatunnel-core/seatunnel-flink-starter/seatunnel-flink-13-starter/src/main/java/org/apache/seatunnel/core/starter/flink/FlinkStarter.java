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

package org.apache.seatunnel.core.starter.flink;

import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.core.starter.Starter;
import org.apache.seatunnel.core.starter.enums.EngineType;
import org.apache.seatunnel.core.starter.flink.args.FlinkCommandArgs;
import org.apache.seatunnel.core.starter.utils.CommandLineUtils;
import org.apache.seatunnel.core.starter.utils.SystemUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** The SeaTunnel flink starter, used to generate the final flink job execute command. */
public class FlinkStarter implements Starter {
    private static final String APP_NAME = SeaTunnelFlink.class.getName();
    public static final String APP_JAR_NAME = EngineType.FLINK13.getStarterJarName();
    public static final String SHELL_NAME = EngineType.FLINK13.getStarterShellName();
    private final FlinkCommandArgs flinkCommandArgs;
    private final String appJar;

    FlinkStarter(String[] args) {
        this.flinkCommandArgs =
                CommandLineUtils.parse(args, new FlinkCommandArgs(), SHELL_NAME, true);
        // set the deployment mode, used to get the job jar path.
        Common.setDeployMode(flinkCommandArgs.getDeployMode());
        Common.setStarter(true);
        this.appJar = Common.appStarterDir().resolve(APP_JAR_NAME).toString();
    }

    public static void main(String[] args) {
        FlinkStarter flinkStarter = new FlinkStarter(args);
        System.out.println(String.join(" ", flinkStarter.buildCommands()));
    }

    @Override
    public List<String> buildCommands() {
        List<String> command = new ArrayList<>();
        String local_os_type = "";

        SystemUtil my_system_util = new SystemUtil();
        local_os_type = my_system_util.GetOsType();

        // set start command
        String cmd_flink = "";

        // Nothe that "flink.cmd” or "flink.bat" can be retrieved from lower version of flink (e.g.
        // 1.0.9)
        // We do not check if this file exists on the box, user needs to make sure this file exists
        // or not.
        switch (local_os_type.toLowerCase()) {
            case "windows":
                cmd_flink = "%FLINK_HOME%/bin/flink.cmd";
                break;
            case "linux":
                cmd_flink = "${FLINK_HOME}/bin/flink";
                break;
            case "solaris":
                cmd_flink = "${FLINK_HOME}/bin/flink";
                break;
            case "mac":
                cmd_flink = "${FLINK_HOME}/bin/flink";
                break;
            case "unknown":
                cmd_flink = "error";
                break;
        }

        // set start command
        if (!(cmd_flink.equals("error"))) {
            command.add(cmd_flink);
        } else {
            System.out.println("Error: Can not determine OS type, abort run !");
            System.exit(-1);
        }

        // set deploy mode, run or run-application
        command.add(flinkCommandArgs.getDeployMode().getDeployMode());
        // set submitted target master
        if (flinkCommandArgs.getMasterType() != null) {
            command.add("--target");
            command.add(flinkCommandArgs.getMasterType().getMaster());
        }
        // set flink original parameters
        command.addAll(flinkCommandArgs.getOriginalParameters());
        // set main class name
        command.add("-c");
        command.add(APP_NAME);
        // set main jar name
        command.add(appJar);
        // set config file path
        command.add("--config");
        command.add(flinkCommandArgs.getConfigFile());
        // set check config flag
        if (flinkCommandArgs.isCheckConfig()) {
            command.add("--check");
        }
        // set job name
        command.add("--name");
        command.add(flinkCommandArgs.getJobName());
        // set encryption
        if (flinkCommandArgs.isEncrypt()) {
            command.add("--encrypt");
        }
        // set decryption
        if (flinkCommandArgs.isDecrypt()) {
            command.add("--decrypt");
        }
        // set extra system properties
        flinkCommandArgs.getVariables().stream()
                .filter(Objects::nonNull)
                .map(String::trim)
                .forEach(variable -> command.add("-D" + variable));
        // debug
        // System.out.println("Whole command string:" + command.toString());
        return command;
    }
}
