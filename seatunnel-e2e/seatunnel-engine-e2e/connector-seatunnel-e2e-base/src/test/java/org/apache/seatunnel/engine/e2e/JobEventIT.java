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

package org.apache.seatunnel.engine.e2e;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JobEventIT extends SeaTunnelEngineContainer {

    /** When testing job recovery, is it successful to reapply for resources */
    @Test
    public void testJobRestoreApplyResources() throws IOException, InterruptedException {
        Container.ExecResult execResult = executeJob(server, "/fakesource_to_console.conf");
        Assertions.assertTrue(execResult.getStdout().contains("LIFECYCLE_READER_OPEN"));

        String regex = "job id:\\s*(\\d+)";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(server.getLogs());
        String jobId = null;
        if (matcher.find()) {
            jobId = matcher.group(1);
        }
        Assertions.assertNotNull(jobId);
        Container.ExecResult execResult1 =
                server.execInContainer("sh", "-c", "curl localhost:8080/event/" + jobId);
        Assertions.assertTrue(execResult1.getStdout().contains("LIFECYCLE_READER_OPEN"));
    }

}
