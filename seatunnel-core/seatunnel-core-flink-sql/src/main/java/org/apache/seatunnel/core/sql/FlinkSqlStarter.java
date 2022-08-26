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

package org.apache.seatunnel.core.sql;

import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.core.base.Starter;
import org.apache.seatunnel.core.base.utils.CommandLineUtils;
import org.apache.seatunnel.core.flink.args.FlinkCommandArgs;
import org.apache.seatunnel.core.flink.config.FlinkJobType;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class FlinkSqlStarter implements Starter {

    private static final String APP_JAR_NAME = "seatunnel-core-flink-sql.jar";
    private static final String CLASS_NAME = SeatunnelSql.class.getName();

    private final FlinkCommandArgs flinkCommandArgs;
    /**
     * SeaTunnel flink sql job jar.
     */
    private final String appJar;

    FlinkSqlStarter(String[] args) {
        this.flinkCommandArgs = CommandLineUtils.parse(args, new FlinkCommandArgs(), FlinkJobType.SQL.getType(), true);
        // set the deployment mode, used to get the job jar path.
        Common.setStarter(true);
        Common.setDeployMode(flinkCommandArgs.getDeployMode());
        this.appJar = Common.appLibDir().resolve(APP_JAR_NAME).toString();
    }

    @Override
    public List<String> buildCommands() {
        List<String> command = new ArrayList<>();
        command.add("${FLINK_HOME}/bin/flink");
        command.add(flinkCommandArgs.getRunMode().getMode());
        command.addAll(flinkCommandArgs.getOriginalParameters());
        command.add("-c");
        command.add(CLASS_NAME);
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

    @SuppressWarnings("checkstyle:RegexpSingleline")
    public static void main(String[] args) {
        FlinkSqlStarter flinkSqlStarter = new FlinkSqlStarter(args);
        System.out.println(String.join(" ", flinkSqlStarter.buildCommands()));
    }
}
