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

import org.apache.seatunnel.core.flink.args.FlinkCommandArgs;
import org.apache.seatunnel.core.flink.config.FlinkJobType;
import org.apache.seatunnel.core.flink.config.FlinkRunMode;

import org.junit.Assert;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.List;

public class CommandLineUtilsTest {
    static final String APP_CONF_PATH = ClassLoader.getSystemResource("app.conf").getPath();
    static final String SQL_CONF_PATH = ClassLoader.getSystemResource("sql.conf").getPath();

    @Test
    public void testParseCommandArgs() {
        String[] args = {"--detached", "-c", "app.conf", "-ck", "-i", "city=shenyang", "-i", "date=20200202",
            "-r", "run-application", "--unkown", "unkown-command"};
        FlinkCommandArgs flinkCommandArgs = CommandLineUtils.parseCommandArgs(args, FlinkJobType.JAR);
        Assert.assertEquals(flinkCommandArgs.getFlinkParams(), Arrays.asList("--detached", "--unkown", "unkown-command"));
        Assert.assertEquals(flinkCommandArgs.getRunMode(), FlinkRunMode.APPLICATION_RUN);
        Assert.assertEquals(flinkCommandArgs.getVariables(), Arrays.asList("city=shenyang", "date=20200202"));

        String[] args1 = {"--detached", "-c", "app.conf", "-ck", "-i", "city=shenyang", "-i", "date=20200202",
            "-r", "run-application", "--unkown", "unkown-command"};
        flinkCommandArgs = CommandLineUtils.parseCommandArgs(args1, FlinkJobType.SQL);
        Assert.assertEquals(flinkCommandArgs.getFlinkParams(), Arrays.asList("--detached", "--unkown", "unkown-command"));
        Assert.assertEquals(flinkCommandArgs.getRunMode(), FlinkRunMode.APPLICATION_RUN);
        Assert.assertEquals(flinkCommandArgs.getVariables(), Arrays.asList("city=shenyang", "date=20200202"));
    }

    @Test
    public void testBuildFlinkJarCommand() throws FileNotFoundException {
        String[] args = {"--detached", "-c", APP_CONF_PATH, "-ck", "-i", "city=shenyang", "-i", "date=20200202",
            "-r", "run-application", "--unkown", "unkown-command"};
        FlinkCommandArgs flinkCommandArgs = CommandLineUtils.parseCommandArgs(args, FlinkJobType.JAR);
        List<String> commands = CommandLineUtils.buildFlinkCommand(flinkCommandArgs, "CLASS_NAME", "/path/to/jar");
        Assert.assertEquals(commands,
                Arrays.asList("${FLINK_HOME}/bin/flink", "run-application", "--detached", "--unkown", "unkown-command", "-c",
                        "CLASS_NAME", "/path/to/jar", "--config", APP_CONF_PATH, "--check", "-Dcity=shenyang", "-Ddate=20200202"));

        flinkCommandArgs = CommandLineUtils.parseCommandArgs(args, FlinkJobType.JAR);
        commands = CommandLineUtils.buildFlinkCommand(flinkCommandArgs, "CLASS_NAME", "/path/to/jar");
        Assert.assertEquals(commands,
            Arrays.asList("${FLINK_HOME}/bin/flink", "run-application", "--detached", "--unkown", "unkown-command", "-c",
                "CLASS_NAME", "/path/to/jar", "--config", APP_CONF_PATH, "--check", "-Dcity=shenyang", "-Ddate=20200202"));

        String[] args1 = {"--detached", "-c", "app.conf", "-ck", "-i", "city=shenyang", "-i", "date=20200202",
            "-r", "run-application", "--unkown", "unkown-command"};

    }

    @Test
    public void testBuildFlinkSQLCommand() throws FileNotFoundException{
        String[] args = {"--detached", "-c", SQL_CONF_PATH, "-ck", "-i", "city=shenyang", "-i", "date=20200202",
            "-r", "run-application", "--unkown", "unkown-command"};
        FlinkCommandArgs flinkCommandArgs = CommandLineUtils.parseCommandArgs(args, FlinkJobType.SQL);
        List<String> commands = CommandLineUtils.buildFlinkCommand(flinkCommandArgs, "CLASS_NAME", "/path/to/jar");
        Assert.assertEquals(commands,
                Arrays.asList("${FLINK_HOME}/bin/flink", "run-application", "--detached", "--unkown", "unkown-command", "-c",
                        "CLASS_NAME", "/path/to/jar", "--config", SQL_CONF_PATH, "--check", "-Dcity=shenyang", "-Ddate=20200202"));
    }
}
