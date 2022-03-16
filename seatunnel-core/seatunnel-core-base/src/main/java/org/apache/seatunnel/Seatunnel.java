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

package org.apache.seatunnel;

import org.apache.seatunnel.command.Command;
import org.apache.seatunnel.command.CommandArgs;
import org.apache.seatunnel.command.CommandFactory;
import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.config.ConfigRuntimeException;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Seatunnel {
    private static final Logger LOGGER = LoggerFactory.getLogger(Seatunnel.class);

    /**
     * This method is the entrypoint of SeaTunnel.
     *
     * @param commandArgs commandArgs
     * @param <T> commandType
     */
    public static <T extends CommandArgs> void run(T commandArgs) {

        if (!Common.setDeployMode(commandArgs.getDeployMode().getName())) {
            throw new IllegalArgumentException(
                String.format("Deploy mode: %s is Illegal", commandArgs.getDeployMode()));
        }

        try {
            Command<T> command = CommandFactory.createCommand(commandArgs);
            command.execute(commandArgs);
        } catch (ConfigRuntimeException e) {
            showConfigError(e);
            throw e;
        } catch (Exception e) {
            showFatalError(e);
            throw e;
        }
    }

    private static void showConfigError(Throwable throwable) {
        LOGGER.error(
            "\n\n===============================================================================\n\n");
        String errorMsg = throwable.getMessage();
        LOGGER.error("Config Error:\n");
        LOGGER.error("Reason: {} \n", errorMsg);
        LOGGER.error(
            "\n===============================================================================\n\n\n");
    }

    private static void showFatalError(Throwable throwable) {
        LOGGER.error(
            "\n\n===============================================================================\n\n");
        String errorMsg = throwable.getMessage();
        LOGGER.error("Fatal Error, \n");
        // FIX
        LOGGER.error(
            "Please submit bug report in https://github.com/apache/incubator-seatunnel/issues\n");
        LOGGER.error("Reason:{} \n", errorMsg);
        LOGGER.error("Exception StackTrace:{} ", ExceptionUtils.getStackTrace(throwable));
        LOGGER.error(
            "\n===============================================================================\n\n\n");
    }
}
