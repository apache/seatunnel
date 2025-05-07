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

package org.apache.seatunnel.engine.server;

import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import com.hazelcast.instance.impl.HazelcastInstanceFactory;

import static com.github.stefanbirkner.systemlambda.SystemLambda.catchSystemExit;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;

/** Test for {@link SeaTunnelServerStarter} fatal error handling. */
public class SeaTunnelServerStarterTest {

    /**
     * This test verifies that System.exit(1) is called when a fatal error occurs during Hazelcast
     * server initialization.
     */
    @Test
    public void testFatalErrorHandlingInInitializeHazelcastInstance() throws Exception {
        // Use Mockito to mock the static HazelcastInstanceFactory.newHazelcastInstance method
        try (MockedStatic<HazelcastInstanceFactory> mockedStatic =
                Mockito.mockStatic(HazelcastInstanceFactory.class)) {
            // Make the static method throw an Error
            mockedStatic
                    .when(
                            () ->
                                    HazelcastInstanceFactory.newHazelcastInstance(
                                            any(), any(), any(SeaTunnelNodeContext.class)))
                    .thenThrow(new Error("Simulated fatal error"));

            // Expect System.exit(1) to be called and catch it
            int statusCode =
                    catchSystemExit(
                            () -> {
                                try {
                                    // Use reflection to call the private method
                                    java.lang.reflect.Method method =
                                            SeaTunnelServerStarter.class.getDeclaredMethod(
                                                    "initializeHazelcastInstance",
                                                    SeaTunnelConfig.class,
                                                    String.class);
                                    method.setAccessible(true);
                                    method.invoke(null, new SeaTunnelConfig(), "test-instance");
                                } catch (Exception e) {
                                    // Expected - the method should throw an exception
                                }
                            });

            // Verify the exit code is 1
            assertEquals(1, statusCode);
        }
    }

    /**
     * This test verifies that System.exit(1) is called when an OutOfMemoryError occurs during
     * Hazelcast server initialization.
     */
    @Test
    public void testOutOfMemoryErrorHandlingInInitializeHazelcastInstance() throws Exception {
        // Use Mockito to mock the static HazelcastInstanceFactory.newHazelcastInstance method
        try (MockedStatic<HazelcastInstanceFactory> mockedStatic =
                Mockito.mockStatic(HazelcastInstanceFactory.class)) {
            // Make the static method throw an OutOfMemoryError
            mockedStatic
                    .when(
                            () ->
                                    HazelcastInstanceFactory.newHazelcastInstance(
                                            any(), any(), any(SeaTunnelNodeContext.class)))
                    .thenThrow(new OutOfMemoryError("Simulated OOM error"));

            // Expect System.exit(1) to be called and catch it
            int statusCode =
                    catchSystemExit(
                            () -> {
                                try {
                                    // Use reflection to call the private method
                                    java.lang.reflect.Method method =
                                            SeaTunnelServerStarter.class.getDeclaredMethod(
                                                    "initializeHazelcastInstance",
                                                    SeaTunnelConfig.class,
                                                    String.class);
                                    method.setAccessible(true);
                                    method.invoke(null, new SeaTunnelConfig(), "test-instance");
                                } catch (Exception e) {
                                    // Expected - the method should throw an exception
                                }
                            });

            // Verify the exit code is 1
            assertEquals(1, statusCode);
        }
    }
}
