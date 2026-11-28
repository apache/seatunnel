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

import org.apache.seatunnel.common.constants.EngineType;
import org.apache.seatunnel.core.starter.SeaTunnel;
import org.apache.seatunnel.core.starter.command.Command;
import org.apache.seatunnel.core.starter.exception.CommandException;
import org.apache.seatunnel.core.starter.spark.args.SparkCommandArgs;
import org.apache.seatunnel.core.starter.utils.CommandLineUtils;

public class SeaTunnelSpark {

    /**
     * Runs the job on the Spark driver.
     *
     * <p>In local mode the tasks run in this JVM. A failed task leaves its sink writer open,
     * because closing it could write the rows it still buffers, so non-daemon threads of the writer
     * could keep the JVM alive after this method ends. A failed job therefore ends the JVM here.
     * {@link SeaTunnel#run} has already logged the failure.
     */
    public static void main(String[] args) throws CommandException {
        SparkCommandArgs sparkCommandArgs =
                CommandLineUtils.parse(
                        args,
                        new SparkCommandArgs(),
                        EngineType.SPARK2.getStarterShellName(),
                        true);
        Command<?> command = sparkCommandArgs.buildCommand();
        try {
            SeaTunnel.run(command);
        } catch (Exception e) {
            if (System.getProperty("spark.master", "").startsWith("local")) {
                // The JVM prints this only when the exception leaves main.
                e.printStackTrace();
                System.exit(1);
            }
            throw e;
        }
    }
}
