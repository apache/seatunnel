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
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;

import com.hazelcast.spi.impl.NodeEngineImpl;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.CompletableFuture;

@Slf4j
public class TaskLogManagerService {

    private final String prefix;
    private String path;

    public TaskLogManagerService(TelemetryLogsConfig log, NodeEngineImpl nodeEngine) {
        this.prefix = log.getPrefix();
        this.path = log.getPath();
    }

    public void initClean() {
        try {
            if (path == null) {
                Path currentPath =
                        Paths.get(
                                TaskLogManagerService.class
                                        .getProtectionDomain()
                                        .getCodeSource()
                                        .getLocation()
                                        .toURI());

                Path realPath = resolveSymlink(currentPath);
                Path dirPath = realPath.getParent().getParent();
                if (dirPath.endsWith("/")) {
                    path = dirPath + "logs";
                } else {
                    path = dirPath + "/logs";
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to get current path", e);
        }
    }

    private static Path resolveSymlink(Path path) throws IOException {
        while (Files.isSymbolicLink(path)) {
            Path linkTarget = Files.readSymbolicLink(path);
            if (!linkTarget.isAbsolute()) {
                path = path.getParent().resolve(linkTarget).normalize();
            } else {
                path = linkTarget;
            }
        }
        return path;
    }

    public PassiveCompletableFuture<?> clean(long jobId) {
        String[] logFiles = getLogFiles(jobId, path);

        return new PassiveCompletableFuture<>(
                CompletableFuture.supplyAsync(
                        () -> {
                            for (String logFile : logFiles) {
                                try {
                                    Files.delete(Paths.get(path + "/" + logFile));
                                } catch (IOException e) {
                                    log.warn("Failed to delete log file: {}", logFile, e);
                                }
                            }
                            return new PassiveCompletableFuture<>(null);
                        }));
    }

    private String[] getLogFiles(long jobId, String path) {
        File logDir = new File(path);
        if (!logDir.exists() || !logDir.isDirectory()) {
            log.warn(
                    "Skipping deletion: Log directory '{}' either does not exist or is not a valid directory. Please verify the path and ensure the logs are being written correctly.",
                    path);
            return new String[0];
        }

        return logDir.list(
                (dir, name) -> {
                    if (name.startsWith(prefix) && name.contains(String.valueOf(jobId))) {
                        return true;
                    }
                    return false;
                });
    }
}
