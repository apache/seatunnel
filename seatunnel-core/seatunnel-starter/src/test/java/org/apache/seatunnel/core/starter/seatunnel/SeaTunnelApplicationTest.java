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

package org.apache.seatunnel.core.starter.seatunnel;

import org.apache.seatunnel.engine.common.runtime.DeployType;
import org.apache.seatunnel.resource.core.application.ApplicationId;
import org.apache.seatunnel.resource.core.application.ApplicationResult;
import org.apache.seatunnel.resource.core.application.ApplicationStatus;
import org.apache.seatunnel.resource.core.client.ApplicationClient;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SeaTunnelApplicationTest {
    @TempDir Path temporary;

    @Test
    void stopsWaitingWhenApplicationNoLongerExists() throws Exception {
        ApplicationClient client = mock(ApplicationClient.class);
        when(client.getResult())
                .thenReturn(
                        new ApplicationResult(
                                new ApplicationId(DeployType.KUBERNETES, "expired-job"),
                                ApplicationStatus.UNKNOWN,
                                "Job no longer exists"));
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        assertEquals(1, SeaTunnelApplication.printResult(client, true, new PrintStream(output)));
        assertTrue(output.toString("UTF-8").contains("Job no longer exists"));
        verify(client, times(1)).getResult();
    }

    @Test
    void loadsNestedHoconOptionsAndRejectsNonScalarValues() throws Exception {
        Path config = temporary.resolve("deployment.conf");
        Files.write(
                config,
                ("application { worker-count = 3 }\nyarn.queue = batch\n"
                                + "\"application.master.memory-mb\" = 2048\n")
                        .getBytes(StandardCharsets.UTF_8));
        Map<String, String> options = SeaTunnelApplication.loadOptions(config.toString());
        assertEquals("3", options.get("application.worker-count"));
        assertEquals("batch", options.get("yarn.queue"));
        assertEquals("2048", options.get("application.master.memory-mb"));
        Files.write(config, "yarn.queue = [batch]".getBytes(StandardCharsets.UTF_8));
        assertThrows(
                IllegalArgumentException.class,
                () -> SeaTunnelApplication.loadOptions(config.toString()));
    }

    @Test
    void validatesCommandBeforeLoadingAnyPlatformProvider() throws Exception {
        PrintStream out = new PrintStream(new ByteArrayOutputStream());
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        SeaTunnelApplication.execute(
                                new String[] {"cancel", "--target", "yarn"}, out));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        SeaTunnelApplication.execute(
                                new String[] {"submit", "--target", "kubernetes"}, out));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        SeaTunnelApplication.execute(
                                new String[] {"status", "--target", "standalone", "--id", "test"},
                                out));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        SeaTunnelApplication.execute(
                                new String[] {"delete", "--target", "yarn", "--id", "test"}, out));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        SeaTunnelApplication.execute(
                                new String[] {
                                    "status",
                                    "--target",
                                    "yarn",
                                    "--id",
                                    "test",
                                    "--restore-from-checkpoint",
                                    "100"
                                },
                                out));
        ByteArrayOutputStream help = new ByteArrayOutputStream();
        assertEquals(
                0, SeaTunnelApplication.execute(new String[] {"--help"}, new PrintStream(help)));
        assertTrue(help.toString("UTF-8").contains("--deployment-config"));
        assertTrue(help.toString("UTF-8").contains("--restore-from-checkpoint"));
    }
}
