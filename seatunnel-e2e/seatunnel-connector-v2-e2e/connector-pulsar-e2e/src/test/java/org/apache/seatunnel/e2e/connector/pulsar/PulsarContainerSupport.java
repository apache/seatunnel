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

package org.apache.seatunnel.e2e.connector.pulsar;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.exception.NotFoundException;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

final class PulsarContainerSupport {

    private static final Logger LOG = LoggerFactory.getLogger(PulsarContainerSupport.class);
    private static final Duration IMAGE_PULL_TIMEOUT = Duration.ofMinutes(10);

    private PulsarContainerSupport() {}

    /**
     * Pre-pulls the Pulsar image before Testcontainers starts the container. This keeps the first
     * CI pull from failing when GitHub runners briefly make no download progress and Testcontainers
     * aborts the pull attempt early.
     */
    static PulsarContainer startPulsarContainer(
            DockerClient dockerClient,
            String imageName,
            Network network,
            String networkAlias,
            Duration startupTimeout)
            throws InterruptedException {
        ensureImageAvailable(dockerClient, imageName);

        PulsarContainer pulsarContainer =
                new PulsarContainer(DockerImageName.parse(imageName))
                        .withNetwork(network)
                        .withNetworkAliases(networkAlias)
                        .withStartupTimeout(startupTimeout)
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(imageName)));

        Startables.deepStart(Stream.of(pulsarContainer)).join();
        return pulsarContainer;
    }

    private static void ensureImageAvailable(DockerClient dockerClient, String imageName)
            throws InterruptedException {
        if (isImageAvailable(dockerClient, imageName)) {
            LOG.info("Reuse local Pulsar image {}", imageName);
            return;
        }

        DockerImageName dockerImageName = DockerImageName.parse(imageName);
        LOG.info(
                "Pre-pulling Pulsar image {} with timeout {} to avoid flaky first-pull failures",
                imageName,
                IMAGE_PULL_TIMEOUT);
        boolean completed =
                dockerClient
                        .pullImageCmd(dockerImageName.getUnversionedPart())
                        .withTag(dockerImageName.getVersionPart())
                        .start()
                        .awaitCompletion(IMAGE_PULL_TIMEOUT.getSeconds(), TimeUnit.SECONDS);
        if (!completed || !isImageAvailable(dockerClient, imageName)) {
            throw new IllegalStateException(
                    String.format(
                            "Failed to pre-pull Pulsar image %s within %s",
                            imageName, IMAGE_PULL_TIMEOUT));
        }
    }

    private static boolean isImageAvailable(DockerClient dockerClient, String imageName) {
        try {
            dockerClient.inspectImageCmd(imageName).exec();
            return true;
        } catch (NotFoundException ignored) {
            return false;
        }
    }
}
