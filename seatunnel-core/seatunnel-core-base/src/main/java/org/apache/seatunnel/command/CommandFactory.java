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

import org.apache.seatunnel.command.flink.FlinkCommandBuilder;
import org.apache.seatunnel.command.spark.SparkCommandBuilder;

public class CommandFactory {

    private CommandFactory() {
    }

    /**
     * Create seatunnel command.
     *
     * @param commandArgs command args.
     * @return Special command.s
     */
    @SuppressWarnings("unchecked")
    public static <T extends CommandArgs> Command<T> createCommand(T commandArgs) {
        switch (commandArgs.getEngineType()) {
            case FLINK:
                return (Command<T>) new FlinkCommandBuilder().buildCommand((FlinkCommandArgs) commandArgs);
            case SPARK:
                return (Command<T>) new SparkCommandBuilder().buildCommand((SparkCommandArgs) commandArgs);
            default:
                throw new RuntimeException(String.format("engine type: %s is not supported", commandArgs.getEngineType()));
        }
    }
}
