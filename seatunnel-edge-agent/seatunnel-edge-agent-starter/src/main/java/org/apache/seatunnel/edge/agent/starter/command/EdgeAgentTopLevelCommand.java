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

package org.apache.seatunnel.edge.agent.starter.command;

import org.apache.seatunnel.edge.agent.starter.command.db.DbCommandArgs;

import java.util.Arrays;
import java.util.function.Supplier;

public enum EdgeAgentTopLevelCommand {
    RUN(null, EdgeAgentStarterConstants.PROGRAM_NAME, RunCommandArgs::new),
    DB("db", EdgeAgentStarterConstants.PROGRAM_NAME_DB, DbCommandArgs::new);

    private final String cliName;
    private final String programName;
    private final Supplier<? extends EdgeAgentCommandArgs> argsSupplier;

    EdgeAgentTopLevelCommand(
            String cliName,
            String programName,
            Supplier<? extends EdgeAgentCommandArgs> argsSupplier) {
        this.cliName = cliName;
        this.programName = programName;
        this.argsSupplier = argsSupplier;
    }

    public static EdgeAgentTopLevelCommand resolve(String[] args) {
        if (args != null && args.length > 0 && DB.cliName.equals(args[0])) {
            return DB;
        }
        return RUN;
    }

    public EdgeAgentCommand<?> buildCommand(String[] rawArgs) {
        EdgeAgentCommandArgs commandArgs =
                EdgeAgentCommandLineUtils.parse(
                        remainingArgs(rawArgs), argsSupplier.get(), programName);
        return commandArgs.buildCommand();
    }

    private String[] remainingArgs(String[] rawArgs) {
        if (this == DB) {
            if (rawArgs == null || rawArgs.length <= 1) {
                return new String[0];
            }
            return Arrays.copyOfRange(rawArgs, 1, rawArgs.length);
        }
        return rawArgs == null ? new String[0] : rawArgs;
    }
}
