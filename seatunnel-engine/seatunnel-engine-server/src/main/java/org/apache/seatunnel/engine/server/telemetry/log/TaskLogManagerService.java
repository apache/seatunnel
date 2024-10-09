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

import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@Slf4j
public class TaskLogManagerService implements AutoCloseable {

    private final String cron;
    private final long keepTime;
    private final String prefix;
    private String path;
    private final NodeEngine nodeEngine;
    private TaskLogCleanService taskLogCleanService;

    public TaskLogManagerService(TelemetryLogsConfig log, NodeEngineImpl nodeEngine) {
        this.cron = log.getCron();
        this.keepTime = log.getKeepTime();
        this.prefix = log.getPrefix();
        this.path = log.getPath();
        this.nodeEngine = nodeEngine;
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
        taskLogCleanService = new TaskLogCleanService(cron, keepTime, prefix, path, nodeEngine);
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

    @Override
    public void close() {
        if (taskLogCleanService != null) {
            taskLogCleanService.shutdown();
        }
    }
}
