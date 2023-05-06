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

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

/** pulsar container */
public class PulsarContainer extends GenericContainer<PulsarContainer> {

    public static final int BROKER_PORT = 6650;

    public static final int BROKER_HTTP_PORT = 8080;

    private static final String ADMIN_CLUSTERS_ENDPOINT = "/admin/v2/clusters";

    private static final String TRANSACTION_TOPIC_ENDPOINT =
            "/admin/v2/persistent/pulsar/system/transaction_coordinator_assign/partitions";

    private final WaitAllStrategy waitAllStrategy = new WaitAllStrategy();

    private boolean functionsWorkerEnabled = false;

    private boolean transactionsEnabled = false;

    public PulsarContainer(final DockerImageName dockerImageName) {
        super(dockerImageName);
        dockerImageName.assertCompatibleWith(DockerImageName.parse("apachepulsar/pulsar"));
        withExposedPorts(BROKER_PORT, BROKER_HTTP_PORT);
        setWaitStrategy(waitAllStrategy);
    }

    @Override
    protected void configure() {
        super.configure();
        setupCommandAndEnv();
    }

    public PulsarContainer withFunctionsWorker() {
        functionsWorkerEnabled = true;
        return this;
    }

    public PulsarContainer withTransactions() {
        transactionsEnabled = true;
        return this;
    }

    public String getPulsarBrokerUrl() {
        return String.format("pulsar://%s:%s", getHost(), getMappedPort(BROKER_PORT));
    }

    public String getHttpServiceUrl() {
        return String.format("http://%s:%s", getHost(), getMappedPort(BROKER_HTTP_PORT));
    }

    protected void setupCommandAndEnv() {
        String standaloneBaseCommand =
                "/pulsar/bin/apply-config-from-env.py /pulsar/conf/standalone.conf "
                        + "&& bin/pulsar standalone";

        if (!functionsWorkerEnabled) {
            standaloneBaseCommand += " --no-functions-worker -nss";
        }

        withCommand("/bin/bash", "-c", standaloneBaseCommand);

        final String clusterName =
                getEnvMap().getOrDefault("PULSAR_PREFIX_clusterName", "standalone");
        final String response = String.format("[\"%s\"]", clusterName);
        waitAllStrategy.withStrategy(
                Wait.forHttp(ADMIN_CLUSTERS_ENDPOINT)
                        .forPort(BROKER_HTTP_PORT)
                        .forResponsePredicate(response::equals));

        if (transactionsEnabled) {
            withEnv("PULSAR_PREFIX_transactionCoordinatorEnabled", "true");
            waitAllStrategy.withStrategy(
                    Wait.forHttp(TRANSACTION_TOPIC_ENDPOINT)
                            .forStatusCode(200)
                            .forPort(BROKER_HTTP_PORT));
        }
        if (functionsWorkerEnabled) {
            waitAllStrategy.withStrategy(
                    Wait.forLogMessage(".*Function worker service started.*", 1));
        }
        waitAllStrategy.withStartupTimeout(Duration.ofMinutes(20));
    }
}
