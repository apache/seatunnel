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

package org.apache.seatunnel.e2e.common.container;

import org.junit.jupiter.api.Assertions;
import org.testcontainers.containers.Container;

import java.io.IOException;

public class TestHelper {
    private final TestContainer container;

    public TestHelper(TestContainer container) {
        this.container = container;
    }

    public void execute(String file) throws IOException, InterruptedException {
        execute(0, file);
    }

    public void execute(int exceptResult, String file) throws IOException, InterruptedException {
        Container.ExecResult result = container.executeJob(file);
        Assertions.assertEquals(exceptResult, result.getExitCode(), result.getStderr());
    }
}
