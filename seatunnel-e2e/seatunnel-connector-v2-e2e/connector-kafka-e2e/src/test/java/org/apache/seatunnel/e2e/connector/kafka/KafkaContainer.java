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

package org.apache.seatunnel.e2e.connector.kafka;

import com.github.dockerjava.api.command.InspectContainerResponse;
import lombok.SneakyThrows;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;

/**
 * This container wraps Confluent Kafka and Zookeeper (optionally)
 */
public class KafkaContainer extends GenericContainer<KafkaContainer> {

    private static final DockerImageName DEFAULT_IMAGE_NAME = DockerImageName.parse("confluentinc/cp-kafka");

    public static final int KAFKA_PORT = 9094;

    public static final int ZOOKEEPER_PORT = 2181;

    private static final String DEFAULT_INTERNAL_TOPIC_RF = "1";

    protected String externalZookeeperConnect = null;

    public KafkaContainer(final DockerImageName dockerImageName) {
        super(dockerImageName);
        dockerImageName.assertCompatibleWith(DEFAULT_IMAGE_NAME);

        withExposedPorts(KAFKA_PORT);

        // Use two listeners with different names, it will force Kafka to communicate with itself via internal
        // listener when KAFKA_INTER_BROKER_LISTENER_NAME is set, otherwise Kafka will try to use the advertised listener
        withEnv("KAFKA_LISTENERS", "PLAINTEXT://0.0.0.0:" + KAFKA_PORT + ",BROKER://0.0.0.0:9092");
        withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "BROKER:PLAINTEXT,PLAINTEXT:PLAINTEXT");
        withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "BROKER");

        withEnv("KAFKA_BROKER_ID", "1");
        withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", DEFAULT_INTERNAL_TOPIC_RF);
        withEnv("KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS", DEFAULT_INTERNAL_TOPIC_RF);
        withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", DEFAULT_INTERNAL_TOPIC_RF);
        withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", DEFAULT_INTERNAL_TOPIC_RF);
        withEnv("KAFKA_LOG_FLUSH_INTERVAL_MESSAGES", Long.MAX_VALUE + "");
        withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0");
    }

    public KafkaContainer withEmbeddedZookeeper() {
        externalZookeeperConnect = null;
        return self();
    }

    public KafkaContainer withExternalZookeeper(String connectString) {
        externalZookeeperConnect = connectString;
        return self();
    }

    public String getBootstrapServers() {
        return String.format("PLAINTEXT://%s:%s", getLinuxLocalIp(), getMappedPort(KAFKA_PORT));
    }

    @Override
    protected void configure() {
        withEnv(
                "KAFKA_ADVERTISED_LISTENERS",
                String.format("BROKER://%s:9092", getNetwork() != null ? getNetworkAliases().get(1) : "localhost")
        );

        String command = "#!/bin/bash\n";
        if (externalZookeeperConnect != null) {
            withEnv("KAFKA_ZOOKEEPER_CONNECT", externalZookeeperConnect);
        } else {
            addExposedPort(ZOOKEEPER_PORT);
            withEnv("KAFKA_ZOOKEEPER_CONNECT", "localhost:" + ZOOKEEPER_PORT);
            command += "echo 'clientPort=" + ZOOKEEPER_PORT + "' > zookeeper.properties\n";
            command += "echo 'dataDir=/var/lib/zookeeper/data' >> zookeeper.properties\n";
            command += "echo 'dataLogDir=/var/lib/zookeeper/log' >> zookeeper.properties\n";
            command += "zookeeper-server-start zookeeper.properties &\n";
        }

        // Optimization: skip the checks
        command += "echo '' > /etc/confluent/docker/ensure \n";
        // Run the original command
        command += "/etc/confluent/docker/run \n";
        withCommand("sh", "-c", command);
    }

    @Override
    @SneakyThrows
    protected void containerIsStarted(InspectContainerResponse containerInfo) {
        String brokerAdvertisedListener = brokerAdvertisedListener(containerInfo);
        ExecResult result = execInContainer(
                "kafka-configs",
                "--alter",
                "--bootstrap-server",
                brokerAdvertisedListener,
                "--entity-type",
                "brokers",
                "--entity-name",
                getEnvMap().get("KAFKA_BROKER_ID"),
                "--add-config",
                "advertised.listeners=[" + String.join(",", getBootstrapServers(), brokerAdvertisedListener) + "]"
        );
        if (result.getExitCode() != 0) {
            throw new IllegalStateException(result.toString());
        }
    }

    protected String brokerAdvertisedListener(InspectContainerResponse containerInfo) {
        return String.format("BROKER://%s:%s", containerInfo.getConfig().getHostName(), "9092");
    }

    public String getLinuxLocalIp() {
        String ip = "";
        try {
            Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
            while (networkInterfaces.hasMoreElements()) {
                NetworkInterface networkInterface = networkInterfaces.nextElement();
                Enumeration<InetAddress> inetAddresses = networkInterface.getInetAddresses();
                while (inetAddresses.hasMoreElements()) {
                    InetAddress inetAddress = inetAddresses.nextElement();
                    if (!inetAddress.isLoopbackAddress() && inetAddress instanceof Inet4Address) {
                        ip =  inetAddress.getHostAddress();
                    }
                }
            }
        } catch (SocketException ex) {
            ex.printStackTrace();
        }
        return ip;
    }
}
