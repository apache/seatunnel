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

package org.apache.seatunnel.connectors.seatunnel.neo4j.source;

import org.apache.seatunnel.connectors.seatunnel.neo4j.config.DriverBuilder;

import org.junit.jupiter.api.Test;
import org.neo4j.driver.Driver;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

class Neo4jSourceDryRunValidatorTest {
    @Test
    void verifiesConnectionAndClosesWithoutCreatingSession() throws Exception {
        DriverBuilder builder = mock(DriverBuilder.class);
        Driver driver = mock(Driver.class);
        when(builder.build()).thenReturn(driver);
        when(driver.verifyConnectivityAsync()).thenReturn(CompletableFuture.completedFuture(null));
        when(driver.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));

        Neo4jSourceDryRunValidator.validate(builder);

        verify(driver).verifyConnectivityAsync();
        verify(driver).closeAsync();
        verifyNoMoreInteractions(driver);
        verify(builder).setMaxConnectionTimeoutSeconds(15L);
        verify(builder).setMaxTransactionRetryTimeSeconds(0L);
    }

    @Test
    void closesAfterAuthenticationFailureAndDoesNotExposeDriverText() {
        DriverBuilder builder = mock(DriverBuilder.class);
        Driver driver = mock(Driver.class);
        CompletableFuture<Void> failed = new CompletableFuture<>();
        failed.completeExceptionally(new IllegalArgumentException("neo4j://user:secret@host"));
        when(builder.build()).thenReturn(driver);
        when(driver.verifyConnectivityAsync()).thenReturn(failed);
        when(driver.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));

        IOException error =
                assertThrows(IOException.class, () -> Neo4jSourceDryRunValidator.validate(builder));

        assertFalse(error.getMessage().contains("secret"));
        assertNull(error.getCause());
        assertEquals(0, error.getSuppressed().length);
        verify(driver).closeAsync();
    }

    @Test
    void honorsSmallerTimeoutAndClosesStalledHandshake() {
        DriverBuilder builder = mock(DriverBuilder.class);
        Driver driver = mock(Driver.class);
        when(builder.getMaxConnectionTimeoutSeconds()).thenReturn(1L);
        when(builder.build()).thenReturn(driver);
        when(driver.verifyConnectivityAsync()).thenReturn(new CompletableFuture<>());
        when(driver.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));

        assertThrows(IOException.class, () -> Neo4jSourceDryRunValidator.validate(builder));
        verify(builder).setMaxConnectionTimeoutSeconds(1L);
        verify(driver).closeAsync();
    }

    @Test
    void rejectsNegativeTimeoutBeforeDriverCreation() {
        DriverBuilder builder = mock(DriverBuilder.class);
        when(builder.getMaxConnectionTimeoutSeconds()).thenReturn(-1L);
        assertThrows(IOException.class, () -> Neo4jSourceDryRunValidator.validate(builder));
        verify(builder).getMaxConnectionTimeoutSeconds();
        verifyNoMoreInteractions(builder);
    }

    @Test
    void rejectsPreInterruptedThreadWithoutCreatingDriver() {
        DriverBuilder builder = mock(DriverBuilder.class);
        Thread.currentThread().interrupt();
        try {
            assertThrows(
                    InterruptedException.class, () -> Neo4jSourceDryRunValidator.validate(builder));
            assertTrue(Thread.currentThread().isInterrupted());
            verifyNoInteractions(builder);
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void preservesInterruptionDuringVerificationAndStillCloses() {
        DriverBuilder builder = mock(DriverBuilder.class);
        Driver driver = mock(Driver.class);
        when(builder.build()).thenReturn(driver);
        when(driver.verifyConnectivityAsync())
                .thenAnswer(
                        ignored -> {
                            Thread.currentThread().interrupt();
                            return new CompletableFuture<>();
                        });
        when(driver.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));
        try {
            assertThrows(
                    InterruptedException.class, () -> Neo4jSourceDryRunValidator.validate(builder));
            assertTrue(Thread.currentThread().isInterrupted());
            verify(driver).closeAsync();
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void cleanupFailureCannotTurnIntoSuccessOrExposeSecrets() {
        DriverBuilder builder = mock(DriverBuilder.class);
        Driver driver = mock(Driver.class);
        when(builder.build()).thenReturn(driver);
        when(driver.verifyConnectivityAsync()).thenReturn(CompletableFuture.completedFuture(null));
        CompletableFuture<Void> failedClose = new CompletableFuture<>();
        failedClose.completeExceptionally(new IllegalArgumentException("private-token"));
        when(driver.closeAsync()).thenReturn(failedClose);
        IOException failure =
                assertThrows(IOException.class, () -> Neo4jSourceDryRunValidator.validate(builder));
        assertTrue(failure.getMessage().contains("cleanup"));
        assertFalse(failure.getMessage().contains("private-token"));
        assertNull(failure.getCause());
    }
}
