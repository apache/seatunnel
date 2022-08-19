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

import org.apache.seatunnel.core.base.utils.CommandLineUtils;
import org.apache.seatunnel.core.flink.args.FlinkCommandArgs;
import org.apache.seatunnel.core.flink.config.FlinkJobType;
import org.apache.seatunnel.core.flink.config.FlinkRunMode;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

public class CommandLineUtilsTest {

    @Test
    public void testParseCommandArgs() {
        String[] args = {"--detached", "-c", "app.conf", "-ck", "-i", "city=shenyang", "-i", "date=20200202",
            "-r", "run-application", "--unkown", "unkown-command"};
        FlinkCommandArgs flinkCommandArgs = CommandLineUtils.parse(args, new FlinkCommandArgs(), FlinkJobType.JAR.getType(), true);
        Assertions.assertEquals(flinkCommandArgs.getOriginalParameters(), Arrays.asList("--detached", "--unkown", "unkown-command"));
        Assertions.assertEquals(flinkCommandArgs.getRunMode(), FlinkRunMode.APPLICATION_RUN);
        Assertions.assertEquals(flinkCommandArgs.getVariables(), Arrays.asList("city=shenyang", "date=20200202"));

        String[] args1 = {"--detached", "-c", "app.conf", "-ck", "-i", "city=shenyang", "-i", "date=20200202",
            "-r", "run-application", "--unkown", "unkown-command"};
        flinkCommandArgs = CommandLineUtils.parse(args1, new FlinkCommandArgs(), FlinkJobType.SQL.getType(), true);
        Assertions.assertEquals(flinkCommandArgs.getOriginalParameters(), Arrays.asList("--detached", "--unkown", "unkown-command"));
        Assertions.assertEquals(flinkCommandArgs.getRunMode(), FlinkRunMode.APPLICATION_RUN);
        Assertions.assertEquals(flinkCommandArgs.getVariables(), Arrays.asList("city=shenyang", "date=20200202"));
    }

}
