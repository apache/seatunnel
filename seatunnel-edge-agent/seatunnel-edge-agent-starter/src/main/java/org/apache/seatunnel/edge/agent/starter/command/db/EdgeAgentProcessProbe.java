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

package org.apache.seatunnel.edge.agent.starter.command.db;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

public class EdgeAgentProcessProbe {

    private static final Path PROC_ROOT = Paths.get("/proc");

    public static boolean isAgentRunning(Path pidFile) throws IOException {
        if (!Files.isRegularFile(pidFile)) {
            return false;
        }
        String raw = readFileAsString(pidFile).trim();
        if (raw.isEmpty() || !raw.matches("[0-9]+")) {
            return false;
        }
        long pid = Long.parseLong(raw);
        return isProcessAlive(pid);
    }

    private static String readFileAsString(Path path) throws IOException {
        return new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
    }

    private static boolean isProcessAlive(long pid) {
        if (pid <= 0) {
            return false;
        }
        if (isCurrentProcess(pid)) {
            return true;
        }
        Boolean processHandleAlive = isProcessAliveWithProcessHandle(pid);
        if (processHandleAlive != null) {
            return processHandleAlive;
        }
        if (Files.isDirectory(PROC_ROOT)) {
            return Files.exists(PROC_ROOT.resolve(String.valueOf(pid)));
        }
        return false;
    }

    private static boolean isCurrentProcess(long pid) {
        String runtimeName = ManagementFactory.getRuntimeMXBean().getName();
        int hostSeparator = runtimeName.indexOf('@');
        if (hostSeparator <= 0) {
            return false;
        }
        try {
            return Long.parseLong(runtimeName.substring(0, hostSeparator)) == pid;
        } catch (NumberFormatException ex) {
            return false;
        }
    }

    private static Boolean isProcessAliveWithProcessHandle(long pid) {
        try {
            Class<?> processHandleClass = Class.forName("java.lang.ProcessHandle");
            Method of = processHandleClass.getMethod("of", long.class);
            Optional<?> optionalHandle = (Optional<?>) of.invoke(null, pid);
            if (!optionalHandle.isPresent()) {
                return false;
            }
            Method isAlive = processHandleClass.getMethod("isAlive");
            return (Boolean) isAlive.invoke(optionalHandle.get());
        } catch (ClassNotFoundException ex) {
            return null;
        } catch (ReflectiveOperationException | RuntimeException ex) {
            return false;
        }
    }
}
