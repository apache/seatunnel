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

package org.apache.seatunnel.engine.imap.storage.file;

import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import com.microsoft.azure.storage.CloudStorageAccount;

/**
 * Azurite (the Azure Storage emulator) for the WASB tests of the {@code azure} storage type.
 *
 * <p>hadoop-azure's emulator mode, selected by the account name {@code storageemulator} in the
 * container URI, always connects to {@code http://127.0.0.1:10000/devstoreaccount1}. The container
 * therefore binds exactly that host address and port for the lifetime of the test class, and fails
 * with an explicit message when it cannot. Only the public development storage account is used: no
 * real credential or endpoint is configured, read or printed.
 *
 * <p>An identical copy lives in {@code checkpoint-storage-hdfs} ({@code
 * org.apache.seatunnel.engine.checkpoint.storage.hdfs.AzuriteEmulator}). Keep the two in sync.
 */
final class AzuriteEmulator implements AutoCloseable {

    /** The container URI that hadoop-azure maps to the local emulator. */
    static final String CONTAINER_URI = "wasb://seatunnel@storageemulator/";

    private static final String CONTAINER_NAME = "seatunnel";
    private static final String HOST = "127.0.0.1";
    private static final int PORT = 10000;
    private static final int CONTAINER_CREATE_ATTEMPTS = 10;
    private static final long CONTAINER_CREATE_RETRY_MILLIS = 500L;
    private static final DockerImageName IMAGE =
            DockerImageName.parse("mcr.microsoft.com/azure-storage/azurite:3.33.0");

    private final GenericContainer<?> container;

    private AzuriteEmulator(GenericContainer<?> container) {
        this.container = container;
    }

    /** Used by {@code @EnabledIf}: the emulator needs a Docker daemon. */
    static boolean isDockerAvailable() {
        try {
            return DockerClientFactory.instance().isDockerAvailable();
        } catch (Throwable e) {
            return false;
        }
    }

    /**
     * Starts the emulator on 127.0.0.1:10000 and creates the test container, which hadoop-azure
     * does not create on first write.
     */
    static AzuriteEmulator start() {
        GenericContainer<?> azurite =
                new GenericContainer<>(IMAGE)
                        .withCommand(
                                "azurite-blob",
                                "--blobHost",
                                "0.0.0.0",
                                "--blobPort",
                                String.valueOf(PORT),
                                "--skipApiVersionCheck",
                                "--loose")
                        .withExposedPorts(PORT)
                        .withCreateContainerCmdModifier(
                                cmd ->
                                        cmd.getHostConfig()
                                                .withPortBindings(
                                                        new PortBinding(
                                                                Ports.Binding.bindIpAndPort(
                                                                        HOST, PORT),
                                                                new ExposedPort(PORT))))
                        .waitingFor(Wait.forListeningPort());
        try {
            azurite.start();
        } catch (RuntimeException e) {
            azurite.close();
            throw new IllegalStateException(
                    "Could not start Azurite on "
                            + HOST
                            + ":"
                            + PORT
                            + ". hadoop-azure's emulator mode only connects to this address,"
                            + " so the port must be free while the Azure storage tests run.",
                    e);
        }
        // The port can accept connections shortly before the blob service is ready.
        Exception lastError = null;
        for (int attempt = 1; attempt <= CONTAINER_CREATE_ATTEMPTS; attempt++) {
            try {
                CloudStorageAccount.getDevelopmentStorageAccount()
                        .createCloudBlobClient()
                        .getContainerReference(CONTAINER_NAME)
                        .createIfNotExists();
                return new AzuriteEmulator(azurite);
            } catch (Exception e) {
                lastError = e;
                if (attempt < CONTAINER_CREATE_ATTEMPTS) {
                    try {
                        Thread.sleep(CONTAINER_CREATE_RETRY_MILLIS);
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }
        azurite.close();
        throw new IllegalStateException(
                "Azurite started but the test container could not be created on "
                        + HOST
                        + ":"
                        + PORT
                        + " after "
                        + CONTAINER_CREATE_ATTEMPTS
                        + " attempts",
                lastError);
    }

    @Override
    public void close() {
        container.close();
    }
}
