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

package org.apache.seatunnel.core.starter.spark;

import org.apache.seatunnel.core.starter.Seatunnel;
import org.apache.seatunnel.core.starter.command.Command;
import org.apache.seatunnel.core.starter.exception.CommandException;
import org.apache.seatunnel.core.starter.spark.args.SparkCommandArgs;
import org.apache.seatunnel.core.starter.spark.command.SparkCommandBuilder;
import org.apache.seatunnel.core.starter.spark.config.StarterConstant;
import org.apache.seatunnel.core.starter.utils.CommandLineUtils;

public class SeatunnelSpark {

    public static void main(String[] args) throws CommandException {
        SparkCommandArgs sparkArgs = CommandLineUtils.parse(args, new SparkCommandArgs(), StarterConstant.SHELL_NAME, true);
        Command<SparkCommandArgs> sparkCommand =
            new SparkCommandBuilder().buildCommand(sparkArgs);
        Seatunnel.run(sparkCommand);
    }
}
