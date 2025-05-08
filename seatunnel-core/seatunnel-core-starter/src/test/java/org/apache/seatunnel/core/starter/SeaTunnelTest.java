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

package org.apache.seatunnel.core.starter;

import org.apache.seatunnel.core.starter.command.Command;
import org.apache.seatunnel.core.starter.command.CommandArgs;

import org.junit.jupiter.api.Test;

import static com.github.stefanbirkner.systemlambda.SystemLambda.catchSystemExit;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

/** Test for {@link SeaTunnel} error handling and System.exit behavior. */
public class SeaTunnelTest {

    /**
     * This test verifies that System.exit(1) is called when an OutOfMemoryError occurs during
     * command execution.
     */
    @Test
    public void testOutOfMemoryErrorHandling() throws Exception {
        // Create a mock Command that will throw an OutOfMemoryError
        @SuppressWarnings("unchecked")
        Command<CommandArgs> mockCommand = mock(Command.class);
        doThrow(new OutOfMemoryError("Simulated OOM error")).when(mockCommand).execute();

        // Expect System.exit(1) to be called and catch it
        int statusCode =
                catchSystemExit(
                        () -> {
                            try {
                                SeaTunnel.run(mockCommand);
                            } catch (Throwable e) {
                                // We expect this error to be thrown after System.exit is called
                                // but the exit is intercepted by catchSystemExit
                            }
                        });

        // Verify the exit code is 1
        assertEquals(1, statusCode);
    }

    /** This test verifies that no exception is thrown when command executes successfully. */
    @Test
    public void testSuccessfulExecution() throws Exception {
        // Create a mock Command that will execute successfully
        @SuppressWarnings("unchecked")
        Command<CommandArgs> mockCommand = mock(Command.class);

        // No exception should be thrown
        SeaTunnel.run(mockCommand);
    }
}
