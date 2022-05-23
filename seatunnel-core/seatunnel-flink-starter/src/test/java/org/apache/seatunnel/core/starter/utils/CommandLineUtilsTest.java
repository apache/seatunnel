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

package org.apache.seatunnel.core.starter.utils;

import org.apache.seatunnel.core.starter.args.FlinkCommandArgs;
import org.apache.seatunnel.core.starter.config.FlinkJobType;
import org.apache.seatunnel.core.starter.config.FlinkRunMode;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class CommandLineUtilsTest {

    @Test
    public void testParseCommandArgs() {
        String[] args = {"--detached", "-c", "app.conf", "-t", "-i", "city=shenyang", "-i", "date=20200202",
            "-r", "run-application", "--unkown", "unkown-command"};
        FlinkCommandArgs flinkCommandArgs = CommandLineUtils.parseCommandArgs(args, FlinkJobType.JAR);
        Assert.assertEquals(Arrays.asList("--detached", "--unkown", "unkown-command"), flinkCommandArgs.getFlinkParams());
        Assert.assertEquals(FlinkRunMode.APPLICATION_RUN, flinkCommandArgs.getRunMode());
        Assert.assertEquals(Arrays.asList("city=shenyang", "date=20200202"), flinkCommandArgs.getVariables());

        String[] args1 = {"--detached", "-c", "app.conf", "-t", "-i", "city=shenyang", "-i", "date=20200202",
            "-r", "run-application", "--unkown", "unkown-command"};
        flinkCommandArgs = CommandLineUtils.parseCommandArgs(args1, FlinkJobType.SQL);
        Assert.assertEquals(Arrays.asList("--detached", "--unkown", "unkown-command"), flinkCommandArgs.getFlinkParams());
        Assert.assertEquals(FlinkRunMode.APPLICATION_RUN, flinkCommandArgs.getRunMode());
        Assert.assertEquals(Arrays.asList("city=shenyang", "date=20200202"), flinkCommandArgs.getVariables());
    }

    @Test
    public void testBuildFlinkCommand() {
        String[] args = {"--detached", "-c", "app.conf", "-t", "-i", "city=shenyang", "-i", "date=20200202",
            "-r", "run-application", "--unkown", "unkown-command"};
        FlinkCommandArgs flinkCommandArgs = CommandLineUtils.parseCommandArgs(args, FlinkJobType.JAR);
        List<String> commands = CommandLineUtils.buildFlinkCommand(flinkCommandArgs, "CLASS_NAME", "/path/to/jar");
        Assert.assertEquals(Arrays.asList("${FLINK_HOME}/bin/flink", "run-application", "--detached", "--unkown", "unkown-command", "-c",
                        "CLASS_NAME", "/path/to/jar", "--config", "app.conf", "--check", "-Dcity=shenyang", "-Ddate=20200202"),
                commands);

        flinkCommandArgs = CommandLineUtils.parseCommandArgs(args, FlinkJobType.SQL);
        commands = CommandLineUtils.buildFlinkCommand(flinkCommandArgs, "CLASS_NAME", "/path/to/jar");
        Assert.assertEquals(Arrays.asList("${FLINK_HOME}/bin/flink", "run-application", "--detached", "--unkown", "unkown-command", "-c",
                        "CLASS_NAME", "/path/to/jar", "--config", "app.conf", "--check", "-Dcity=shenyang", "-Ddate=20200202"),
                commands);

    }
}
