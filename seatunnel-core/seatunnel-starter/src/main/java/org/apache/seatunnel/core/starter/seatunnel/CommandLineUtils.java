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

package org.apache.seatunnel.core.starter.seatunnel;

import org.apache.seatunnel.core.starter.seatunnel.args.ClientCommandArgs;
import org.apache.seatunnel.core.starter.seatunnel.args.ServerCommandArgs;
import org.apache.seatunnel.core.starter.seatunnel.constant.SeaTunnelConstant;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.UnixStyleUsageFormatter;

public class CommandLineUtils {

    private static final String CLIENT_SHELL_NAME = "seatunnel.sh";

    private static final String SERVER_SHELL_NAME = "seatunnel.sh";

    private CommandLineUtils() {
        throw new UnsupportedOperationException("CommandLineUtils is a utility class and cannot be instantiated");
    }

    public static ClientCommandArgs parseSeaTunnelClientArgs(String[] args) {
        ClientCommandArgs clientCommandArgs = new ClientCommandArgs();
        JCommander jCommander = getJCommander(CLIENT_SHELL_NAME, args, clientCommandArgs);
        // The args is not belongs to seatunnel, add into flink params
        clientCommandArgs.setSeatunnelParams(jCommander.getUnknownOptions());
        if (clientCommandArgs.isHelp()) {
            printHelp(jCommander);
        }
        return clientCommandArgs;
    }

    public static ServerCommandArgs parseSeaTunnelServerArgs(String[] args) {
        ServerCommandArgs serverCommandArgs = new ServerCommandArgs();
        JCommander jCommander = getJCommander(SERVER_SHELL_NAME, args, serverCommandArgs);
        // The args is not belongs to seatunnel, add into flink params
        serverCommandArgs.setSeatunnelParams(jCommander.getUnknownOptions());
        if (serverCommandArgs.isHelp()) {
            printHelp(jCommander);
        }
        return serverCommandArgs;
    }

    private static void printHelp(JCommander jCommander) {
        jCommander.setUsageFormatter(new UnixStyleUsageFormatter(jCommander));
        jCommander.usage();
        System.exit(SeaTunnelConstant.USAGE_EXIT_CODE);
    }

    private static JCommander getJCommander(String shellName, String[] args, Object serverCommandArgs) {
        return JCommander.newBuilder()
            .programName(shellName)
            .addObject(serverCommandArgs)
            .acceptUnknownOptions(true)
            .args(args)
            .build();
    }

}
