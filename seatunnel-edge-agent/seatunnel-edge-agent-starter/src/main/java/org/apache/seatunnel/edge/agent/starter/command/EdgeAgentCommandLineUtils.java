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

import org.apache.seatunnel.edge.agent.starter.command.db.EdgeAgentDbUsage;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;

public class EdgeAgentCommandLineUtils {

    /** Help keeps the historical usage exit code unless the caller opts in to another one. */
    public static <T extends EdgeAgentCommandArgs> T parse(
            String[] args, T obj, String programName) {
        return parse(args, obj, programName, EdgeAgentStarterConstants.USAGE_EXIT_CODE);
    }

    public static <T extends EdgeAgentCommandArgs> T parse(
            String[] args, T obj, String programName, int helpExitCode) {
        JCommander jCommander =
                JCommander.newBuilder().programName(programName).addObject(obj).build();
        try {
            jCommander.parse(args);
        } catch (ParameterException e) {
            System.err.println(e.getLocalizedMessage());
            // argument errors are not a successful help request
            exit(jCommander, obj, EdgeAgentStarterConstants.USAGE_EXIT_CODE);
            return obj;
        }

        if (obj.isHelp()) {
            exit(jCommander, obj, helpExitCode);
        }
        return obj;
    }

    private static void exit(JCommander jCommander, EdgeAgentCommandArgs args, int exitCode) {
        if (args instanceof org.apache.seatunnel.edge.agent.starter.command.db.DbCommandArgs) {
            EdgeAgentDbUsage.printUsage(System.out);
        } else {
            EdgeAgentUsage.printUsage(System.out);
        }
        System.exit(exitCode);
    }
}
