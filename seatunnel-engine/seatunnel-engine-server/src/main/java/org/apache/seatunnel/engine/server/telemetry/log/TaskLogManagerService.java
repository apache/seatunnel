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

package org.apache.seatunnel.engine.server.telemetry.log;

import org.apache.seatunnel.engine.common.config.server.TelemetryLogsConfig;
import org.apache.seatunnel.engine.common.utils.LogUtil;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

@Slf4j
public class TaskLogManagerService {
    private String path;

    public TaskLogManagerService(TelemetryLogsConfig log) {}

    public void initClean() {
        try {
            path = LogUtil.getLogPath();
        } catch (Exception e) {
            log.debug(
                    "The corresponding log file path is not properly configured, please check the log configuration file.",
                    e);
        }
    }

    public void clean(long jobId) {
        log.info("Cleaning logs for jobId: {} , path : {}", jobId, path);
        if (path == null) {
            return;
        }
        String[] logFiles = getLogFiles(jobId, path);
        for (String logFile : logFiles) {
            try {
                Files.delete(Paths.get(path + "/" + logFile));
            } catch (IOException e) {
                log.warn("Failed to delete log file: {}", logFile, e);
            }
        }
    }

    private String[] getLogFiles(long jobId, String path) {
        File logDir = new File(path);
        if (!logDir.exists() || !logDir.isDirectory()) {
            log.warn(
                    "Skipping deletion: Log directory '{}' either does not exist or is not a valid directory. Please verify the path and ensure the logs are being written correctly.",
                    path);
            return new String[0];
        }

        return logDir.list((dir, name) -> name.contains(String.valueOf(jobId)));
    }
}
