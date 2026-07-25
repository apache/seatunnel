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

package org.apache.seatunnel.engine.e2e.console;

import org.apache.seatunnel.engine.e2e.SeaTunnelEngineContainer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FakeSourceToConsoleIT extends SeaTunnelEngineContainer {

    @Test
    public void testFakeSourceToConsoleSink() throws IOException, InterruptedException {
        Container.ExecResult execResult = executeSeaTunnelJob("/fakesource_to_console.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
    }

    @Test
    public void testCommaParams() {
        List<String> variables = new ArrayList<>();
        variables.add("date=2026-07-13");
        variables.add("cols=\"addr1,addr2\"");
        variables.add("required_cols=[date,id,name,addr1]");
        Container.ExecResult execResult = null;
        try {
            execResult =
                    executeSeaTunnelJob("/fakesource_to_console_with_comma_params.conf", variables);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        Assertions.assertEquals(0, execResult.getExitCode());
    }
}
