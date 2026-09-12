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

package org.apache.seatunnel.example.edge.agent;

import org.apache.seatunnel.edge.agent.starter.EdgeAgentStarter;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class SeaTunnelEdgeAgentLocalExample {

    private static final Path SAMPLE_LOG = Paths.get("tmp", "sample.log");
    private static final Path WAL_DB = Paths.get("tmp", "edge-agent-data", "wal.db");
    private static final int SEED_LINE_COUNT = 20;

    static {
        System.setProperty("log4j2.isThreadContextMapInheritable", "true");
    }

    public static void main(String[] args) throws Exception {
        prepareSampleLog();
        String configurePath = args.length > 0 ? args[0] : "/agent-console.yaml";
        EdgeAgentStarter.main(new String[] {"--config", getConfigFile(configurePath)});
    }

    public static String getConfigFile(String configFile)
            throws FileNotFoundException, URISyntaxException {
        URL resource = SeaTunnelEdgeAgentLocalExample.class.getResource(configFile);
        if (resource == null) {
            throw new FileNotFoundException("Can't find config file: " + configFile);
        }
        return Paths.get(resource.toURI()).toString();
    }

    private static void prepareSampleLog() throws IOException {
        Files.createDirectories(SAMPLE_LOG.getParent());
        Files.createDirectories(WAL_DB.getParent());
        String wal = WAL_DB.toString();
        Files.deleteIfExists(WAL_DB);
        Files.deleteIfExists(Paths.get(wal + "-wal"));
        Files.deleteIfExists(Paths.get(wal + "-shm"));

        StringBuilder seed = new StringBuilder();
        for (int line = 1; line <= SEED_LINE_COUNT; line++) {
            seed.append(
                    String.format(
                            "2026-05-19T10:00:%02d INFO edge-agent example line %d/%d%n",
                            line, line, SEED_LINE_COUNT));
        }
        Files.write(
                SAMPLE_LOG,
                seed.toString().getBytes(StandardCharsets.UTF_8),
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE);
    }
}
