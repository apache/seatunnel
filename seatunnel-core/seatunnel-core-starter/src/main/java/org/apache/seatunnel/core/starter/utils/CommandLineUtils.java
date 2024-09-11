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

package org.apache.seatunnel.core.starter.utils;

import org.apache.seatunnel.core.starter.command.CommandArgs;
import org.apache.seatunnel.core.starter.command.UsageFormatter;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;

import java.util.Arrays;
import java.util.List;

import static org.apache.seatunnel.core.starter.constants.SeaTunnelStarterConstants.USAGE_EXIT_CODE;

public class CommandLineUtils {

    private CommandLineUtils() {
        throw new UnsupportedOperationException(
                "CommandLineUtils is a utility class and cannot be instantiated");
    }

    public static <T extends CommandArgs> T parse(String[] args, T obj) {
        return parse(args, obj, null, false);
    }

    public static <T extends CommandArgs> T parse(
            String[] args, T obj, String programName, boolean acceptUnknownOptions) {
        List<String> list = Arrays.asList(args);
        if (list.contains("-can") || list.contains("--cancel-job")) {
            // When acceptUnknown Options is true, the List parameter cannot be parsed.
            // For details, please refer to the official code JCommander.class#DefaultVariableArity
            acceptUnknownOptions = false;
        }
        JCommander jCommander =
                JCommander.newBuilder()
                        .programName(programName)
                        .addObject(obj)
                        .acceptUnknownOptions(acceptUnknownOptions)
                        .build();
        try {
            jCommander.parse(args);
            // The args is not belongs to SeaTunnel, add into engine original parameters
            obj.setOriginalParameters(jCommander.getUnknownOptions());
        } catch (ParameterException e) {
            System.err.println(e.getLocalizedMessage());
            exit(jCommander);
        }

        if (obj.isHelp()) {
            exit(jCommander);
        }
        return obj;
    }

    private static void exit(JCommander jCommander) {
        jCommander.setUsageFormatter(new UsageFormatter(jCommander));
        jCommander.usage();
        System.exit(USAGE_EXIT_CODE);
    }
}
