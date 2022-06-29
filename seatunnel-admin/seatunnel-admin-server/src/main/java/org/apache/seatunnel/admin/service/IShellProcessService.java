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
package org.apache.seatunnel.admin.service;

import org.apache.seatunnel.admin.common.ProcessCleanStream;

import java.io.IOException;
import java.util.concurrent.Future;

public interface IShellProcessService {

    /**
     * shell提交执行
     *
     * @param command 命令
     * @return 任务
     * @throws IOException 异常
     */
    Process exec(String command) throws IOException;

    /**
     * shell提交执行
     *
     * @param commands 命令
     * @return 任务
     * @throws IOException 异常
     */
    Process exec(String[] commands) throws IOException;

    /**
     * 同步等待返回,并判断是否成功
     *
     * @param process shell任务
     * @return 是否成功
     * @throws InterruptedException 异常
     */
    Future<Boolean> waitForResult(Process process) throws InterruptedException;

    ProcessCleanStream printStderrLog(Process exec, String processName);

    ProcessCleanStream printStdoutLog(Process exec, String processName);

}
