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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;

public class EdgeAgentProcessProbe {

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
        String os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
        try {
            Process process;
            if (os.contains("win")) {
                process =
                        new ProcessBuilder(
                                        "cmd.exe", "/c", "tasklist /FI \"PID eq " + pid + "\" /NH")
                                .redirectErrorStream(true)
                                .start();
            } else {
                process =
                        new ProcessBuilder("kill", "-0", String.valueOf(pid))
                                .redirectErrorStream(true)
                                .start();
            }
            return process.waitFor() == 0;
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            return false;
        } catch (IOException ex) {
            return false;
        }
    }
}
