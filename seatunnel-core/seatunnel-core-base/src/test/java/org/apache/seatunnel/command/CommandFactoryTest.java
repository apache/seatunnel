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

package org.apache.seatunnel.command;

import org.apache.seatunnel.command.flink.FlinkConfValidateCommand;
import org.apache.seatunnel.command.flink.FlinkTaskExecuteCommand;
import org.apache.seatunnel.command.spark.SparkConfValidateCommand;
import org.apache.seatunnel.command.spark.SparkTaskExecuteCommand;

import org.junit.Assert;
import org.junit.Test;

public class CommandFactoryTest {

    @Test
    public void testCreateSparkConfValidateCommand() {
        SparkCommandArgs sparkCommandArgs = new SparkCommandArgs();
        sparkCommandArgs.setCheckConfig(true);
        Command<SparkCommandArgs> sparkCommand = CommandFactory.createCommand(sparkCommandArgs);
        Assert.assertEquals(SparkConfValidateCommand.class, sparkCommand.getClass());
    }

    @Test
    public void testCreateSparkExecuteTaskCommand() {
        SparkCommandArgs sparkCommandArgs = new SparkCommandArgs();
        sparkCommandArgs.setCheckConfig(false);
        Command<SparkCommandArgs> sparkCommand = CommandFactory.createCommand(sparkCommandArgs);
        Assert.assertEquals(SparkTaskExecuteCommand.class, sparkCommand.getClass());
    }

    @Test
    public void testCreateFlinkConfValidateCommand() {
        FlinkCommandArgs flinkCommandArgs = new FlinkCommandArgs();
        flinkCommandArgs.setCheckConfig(true);

        Command<FlinkCommandArgs> flinkCommand = CommandFactory.createCommand(flinkCommandArgs);
        Assert.assertEquals(FlinkConfValidateCommand.class, flinkCommand.getClass());
    }

    @Test
    public void testCreateFlinkExecuteTaskCommand() {
        FlinkCommandArgs flinkCommandArgs = new FlinkCommandArgs();
        flinkCommandArgs.setCheckConfig(false);

        Command<FlinkCommandArgs> flinkCommand = CommandFactory.createCommand(flinkCommandArgs);
        Assert.assertEquals(FlinkTaskExecuteCommand.class, flinkCommand.getClass());

    }
}
