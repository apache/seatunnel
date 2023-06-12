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

package org.apache.seatunnel.core.starter;

import org.apache.seatunnel.common.config.ConfigRuntimeException;
import org.apache.seatunnel.core.starter.command.Command;
import org.apache.seatunnel.core.starter.command.CommandArgs;
import org.apache.seatunnel.core.starter.exception.CommandException;

import org.apache.commons.lang3.exception.ExceptionUtils;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SeaTunnel {

    /**
     * This method is the entrypoint of SeaTunnel.
     *
     * @param command commandArgs
     * @param <T> commandType
     */
    public static <T extends CommandArgs> void run(Command<T> command) throws CommandException {
        try {
            command.execute();
        } catch (ConfigRuntimeException e) {
            showConfigError(e);
            throw e;
        } catch (Exception e) {
            showFatalError(e);
            throw e;
        }
    }

    private static void showConfigError(Throwable throwable) {
        log.error(
                "\n\n===============================================================================\n\n");
        String errorMsg = throwable.getMessage();
        log.error("Config Error:\n");
        log.error("Reason: {} \n", errorMsg);
        log.error(
                "\n===============================================================================\n\n\n");
    }

    private static void showFatalError(Throwable throwable) {
        log.error(
                "\n\n===============================================================================\n\n");
        String errorMsg = throwable.getMessage();
        log.error("Fatal Error, \n");
        // FIX
        log.error("Please submit bug report in https://github.com/apache/seatunnel/issues\n");
        log.error("Reason:{} \n", errorMsg);
        log.error("Exception StackTrace:{} ", ExceptionUtils.getStackTrace(throwable));
        log.error(
                "\n===============================================================================\n\n\n");
    }
}
