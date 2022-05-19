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

package org.apache.seatunnel.core.spark.command;

import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.core.base.command.Command;
import org.apache.seatunnel.core.base.command.CommandBuilder;
import org.apache.seatunnel.core.spark.args.SparkCommandArgs;

public class SparkCommandBuilder implements CommandBuilder<SparkCommandArgs> {

    @Override
    public Command<SparkCommandArgs> buildCommand(SparkCommandArgs commandArgs) {
        if (!Common.setDeployMode(commandArgs.getDeployMode().getName())) {
            throw new IllegalArgumentException(
                    String.format("Deploy mode: %s is Illegal", commandArgs.getDeployMode()));
        }
        switch (commandArgs.getApiType()) {
            case ENGINE_API:
                return new SparkApiCommandBuilder().buildCommand(commandArgs);
            case SEATUNNEL_API:
                return new SeaTunnelApiCommandBuilder().buildCommand(commandArgs);
            default:
                throw new IllegalArgumentException("Unsupported API type: " + commandArgs.getApiType());
        }
    }

    /**
     * Used to generate command for engine API.
     */
    private static class SparkApiCommandBuilder extends SparkCommandBuilder {
        @Override
        public Command<SparkCommandArgs> buildCommand(SparkCommandArgs commandArgs) {
            if (!Common.setDeployMode(commandArgs.getDeployMode().getName())) {
                throw new IllegalArgumentException(
                        String.format("Deploy mode: %s is Illegal", commandArgs.getDeployMode()));
            }
            return commandArgs.isCheckConfig() ? new SparkConfValidateCommand(commandArgs)
                    : new SparkTaskExecuteCommand(commandArgs);
        }
    }

    /**
     * Used to generate command for seaTunnel API.
     */
    private static class SeaTunnelApiCommandBuilder extends SparkCommandBuilder {
        @Override
        public Command<SparkCommandArgs> buildCommand(SparkCommandArgs commandArgs) {
            return commandArgs.isCheckConfig() ? new SeaTunnelApiConfValidateCommand(commandArgs)
                    : new SeaTunnelApiTaskExecuteCommand(commandArgs);
        }
    }

}

