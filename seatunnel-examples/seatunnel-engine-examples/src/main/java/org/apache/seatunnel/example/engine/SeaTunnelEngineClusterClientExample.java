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

package org.apache.seatunnel.example.engine;

import org.apache.seatunnel.core.starter.SeaTunnel;
import org.apache.seatunnel.core.starter.seatunnel.args.ClientCommandArgs;

import java.io.FileNotFoundException;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.Collections;

public class SeaTunnelEngineClusterClientExample {

    public static void main(String[] args) throws Exception {
        String id = "834720088434147329";
        String configurePath = "/examples/fake_to_console.conf";

        submit(configurePath, id);

        // list();
        // savepoint(id);
        // restore(configurePath, id);
        // cancel(id);
    }

    public static void list() {
        ClientCommandArgs clientCommandArgs = new ClientCommandArgs();
        clientCommandArgs.setListJob(true);
        SeaTunnel.run(clientCommandArgs.buildCommand());
    }

    public static void submit(String configurePath, String id)
            throws FileNotFoundException, URISyntaxException {
        String configFile = SeaTunnelEngineLocalExample.getTestConfigFile(configurePath);
        ClientCommandArgs clientCommandArgs = new ClientCommandArgs();
        clientCommandArgs.setConfigFile(configFile);
        clientCommandArgs.setCheckConfig(false);
        clientCommandArgs.setJobName(Paths.get(configFile).getFileName().toString());
        clientCommandArgs.setAsync(true);
        clientCommandArgs.setCustomJobId(id);
        SeaTunnel.run(clientCommandArgs.buildCommand());
    }

    public static void restore(String configurePath, String id)
            throws FileNotFoundException, URISyntaxException {
        String configFile = SeaTunnelEngineLocalExample.getTestConfigFile(configurePath);
        ClientCommandArgs clientCommandArgs = new ClientCommandArgs();
        clientCommandArgs.setConfigFile(configFile);
        clientCommandArgs.setRestoreJobId(id);
        clientCommandArgs.setAsync(true);
        SeaTunnel.run(clientCommandArgs.buildCommand());
    }

    public static void savepoint(String id) {
        ClientCommandArgs clientCommandArgs = new ClientCommandArgs();
        clientCommandArgs.setSavePointJobId(id);
        SeaTunnel.run(clientCommandArgs.buildCommand());
    }

    public static void cancel(String id) {
        ClientCommandArgs clientCommandArgs = new ClientCommandArgs();
        clientCommandArgs.setCancelJobId(Collections.singletonList(id));
        SeaTunnel.run(clientCommandArgs.buildCommand());
    }
}
