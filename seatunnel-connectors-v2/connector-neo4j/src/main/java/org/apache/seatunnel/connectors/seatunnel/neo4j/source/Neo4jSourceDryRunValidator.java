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

import org.neo4j.driver.Driver;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/** Verifies the driver's connection only; never creates a session or executes configured Cypher. */
final class Neo4jSourceDryRunValidator {
    private static final long MAX_TIMEOUT_SECONDS = 15;
    private static final long CLOSE_TIMEOUT_SECONDS = 5;

    private Neo4jSourceDryRunValidator() {}

    static void validate(DriverBuilder builder) throws Exception {
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("Neo4j connect dry-run interrupted");
        }
        Driver driver = null;
        Exception failure = null;
        boolean interrupted = false;
        try {
            Long configuredTimeout = builder.getMaxConnectionTimeoutSeconds();
            if (configuredTimeout != null && configuredTimeout < 0) {
                throw new IllegalArgumentException("Invalid connection timeout");
            }
            long timeout =
                    configuredTimeout == null || configuredTimeout == 0
                            ? MAX_TIMEOUT_SECONDS
                            : Math.min(configuredTimeout, MAX_TIMEOUT_SECONDS);
            // This builder belongs only to the preflight; normal source settings are unchanged.
            builder.setMaxConnectionTimeoutSeconds(timeout);
            builder.setMaxTransactionRetryTimeSeconds(0L);
            driver = builder.build();
            // Also bound routing/handshake waits, not just socket connection establishment.
            driver.verifyConnectivityAsync().toCompletableFuture().get(timeout, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            interrupted = true;
            failure = new InterruptedException("Neo4j connect dry-run interrupted");
        } catch (Exception e) {
            failure =
                    new IOException(
                            "Neo4j connect dry-run connectivity check failed; check URI, authentication and TLS settings");
        } finally {
            interrupted |= Thread.interrupted();
            if (driver != null) {
                try {
                    // Close even after a failed or timed-out handshake; do not wait indefinitely.
                    driver.closeAsync()
                            .toCompletableFuture()
                            .get(CLOSE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    interrupted = true;
                    if (failure == null) {
                        failure =
                                new InterruptedException(
                                        "Neo4j connect dry-run cleanup interrupted");
                    }
                } catch (Exception e) {
                    if (failure == null) {
                        failure = new IOException("Neo4j connect dry-run driver cleanup failed");
                    }
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
                if (failure == null) {
                    failure = new InterruptedException("Neo4j connect dry-run interrupted");
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }
}
