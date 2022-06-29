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
package org.apache.seatunnel.admin.service.impl;

import org.apache.seatunnel.admin.common.ProcessCleanStream;
import org.apache.seatunnel.admin.service.IShellProcessService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.concurrent.Future;

/**
 * shell调用服务
 */
@Service
public class ShellProcessServiceImpl implements IShellProcessService {

    private static final Logger logger = LoggerFactory.getLogger(ShellProcessServiceImpl.class);

    public static final String ERROR_TYPE = "error";
    public static final String INFO_TYPE = "info";

    @Override
    public Process exec(String command) throws IOException {
        return Runtime.getRuntime().exec(command);
    }
    @Override
    public Process exec(String[] command) throws IOException {
        return Runtime.getRuntime().exec(command);
    }

    @Override
    @Async
    public Future<Boolean> waitForResult(Process process) throws InterruptedException {
        int exit = process.waitFor();
        if (exit == 0) {
            return new AsyncResult<>(true);
        } else {
            return new AsyncResult<>(false);
        }
    }

    @Override
    public ProcessCleanStream printStderrLog(Process exec, String processName) {
        ProcessCleanStream processCleanStream = new ProcessCleanStream(exec.getErrorStream(), processName, ERROR_TYPE);
        processCleanStream.start();
        return processCleanStream;
    }

    @Override
    public ProcessCleanStream printStdoutLog(Process exec, String processName) {
        ProcessCleanStream processCleanStream = new ProcessCleanStream(exec.getInputStream(), processName, INFO_TYPE);
        processCleanStream.start();
        return processCleanStream;
    }
}
