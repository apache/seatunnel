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

import org.apache.seatunnel.core.starter.SeaTunnel;
import org.apache.seatunnel.core.starter.seatunnel.args.ClientCommandArgs;
import org.apache.seatunnel.core.starter.utils.CommandLineUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import lombok.extern.slf4j.Slf4j;

import static com.github.stefanbirkner.systemlambda.SystemLambda.catchSystemExit;

@Slf4j
public class SeaTunnelClientOOMTest {

    @Test
    public void testHazelcastOOMExitBehavior() throws Exception {
        // Prepare command line arguments
        String[] args = {"--config", "fake_config.conf"};
        ClientCommandArgs clientCommandArgs = new ClientCommandArgs();

        // Mock CommandLineUtils.parse to return our clientCommandArgs
        try (MockedStatic<CommandLineUtils> mockedCommandLineUtils =
                Mockito.mockStatic(CommandLineUtils.class)) {
            mockedCommandLineUtils
                    .when(
                            () ->
                                    CommandLineUtils.parse(
                                            Mockito.any(String[].class),
                                            Mockito.any(ClientCommandArgs.class),
                                            Mockito.anyString(),
                                            Mockito.anyBoolean()))
                    .thenReturn(clientCommandArgs);

            // Mock SeaTunnel.run to throw OutOfMemoryError
            try (MockedStatic<SeaTunnel> mockedSeaTunnel = Mockito.mockStatic(SeaTunnel.class)) {
                // Simulate Hazelcast thread allocation OOM
                OutOfMemoryError oomError =
                        new OutOfMemoryError("Java heap space during Hazelcast thread allocation");

                // Mock run to throw OOM
                mockedSeaTunnel.when(() -> SeaTunnel.run(Mockito.any())).thenThrow(oomError);

                // Test that System.exit(1) is called
                int statusCode =
                        catchSystemExit(
                                () -> {
                                    SeaTunnelClient.main(args);
                                });

                // Verify exit code is 1
                Assertions.assertEquals(1, statusCode);
            }
        }
    }
}
