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

import org.apache.seatunnel.engine.e2e.SeaTunnelContainer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;

import java.io.IOException;

public class TwoFakeSourceToOneConsoleIT extends SeaTunnelContainer {

    @Test
    public void testTwoFakeSourceToOneConsoleSink() throws IOException, InterruptedException {
        Container.ExecResult execResult =
                executeSeaTunnelJob("/two_fakesource_to_one_console.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
    }
}
