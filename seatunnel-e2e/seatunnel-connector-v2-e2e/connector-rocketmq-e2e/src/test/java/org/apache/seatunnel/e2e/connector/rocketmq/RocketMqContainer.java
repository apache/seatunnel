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

package org.apache.seatunnel.e2e.connector.rocketmq;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import com.github.dockerjava.api.command.InspectContainerResponse;
import lombok.SneakyThrows;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

/** rocketmq container */
public class RocketMqContainer extends GenericContainer<RocketMqContainer> {

    public static final int NAMESRV_PORT = 9876;
    public static final int BROKER_PORT = 10911;
    public static final String BROKER_NAME = "broker-a";
    private static final int DEFAULT_BROKER_PERMISSION = 6;

    public RocketMqContainer(DockerImageName image) {
        super(image);
        withExposedPorts(NAMESRV_PORT, BROKER_PORT, BROKER_PORT - 2);
        this.withEnv("JAVA_OPT_EXT", "-Xms512m -Xmx512m");
    }

    @Override
    protected void configure() {
        String command = "#!/bin/bash\n";
        command += "./mqnamesrv &\n";
        command += "./mqbroker -n localhost:" + NAMESRV_PORT;
        withCommand("sh", "-c", command);
    }

    @Override
    @SneakyThrows
    protected void containerIsStarted(InspectContainerResponse containerInfo) {
        List<String> updateBrokerConfigCommands = new ArrayList<>();
        updateBrokerConfigCommands.add(updateBrokerConfig("autoCreateTopicEnable", true));
        updateBrokerConfigCommands.add(updateBrokerConfig("brokerName", BROKER_NAME));
        updateBrokerConfigCommands.add(updateBrokerConfig("brokerIP1", getLinuxLocalIp()));
        updateBrokerConfigCommands.add(
                updateBrokerConfig("listenPort", getMappedPort(BROKER_PORT)));
        updateBrokerConfigCommands.add(
                updateBrokerConfig("brokerPermission", DEFAULT_BROKER_PERMISSION));
        final String command = String.join(" && ", updateBrokerConfigCommands);
        ExecResult result = execInContainer("/bin/sh", "-c", command);
        if (result != null && result.getExitCode() != 0) {
            throw new IllegalStateException(result.toString());
        }
    }

    private String updateBrokerConfig(final String key, final Object val) {
        final String brokerAddr = "localhost:" + BROKER_PORT;
        return "./mqadmin updateBrokerConfig -b " + brokerAddr + " -k " + key + " -v " + val;
    }

    public String getNameSrvAddr() {
        return String.format("%s:%s", getHost(), getMappedPort(NAMESRV_PORT));
    }

    public String getLinuxLocalIp() {
        String ip = "";
        try {
            Enumeration<NetworkInterface> networkInterfaces =
                    NetworkInterface.getNetworkInterfaces();
            while (networkInterfaces.hasMoreElements()) {
                NetworkInterface networkInterface = networkInterfaces.nextElement();
                Enumeration<InetAddress> inetAddresses = networkInterface.getInetAddresses();
                while (inetAddresses.hasMoreElements()) {
                    InetAddress inetAddress = inetAddresses.nextElement();
                    if (!inetAddress.isLoopbackAddress() && inetAddress instanceof Inet4Address) {
                        ip = inetAddress.getHostAddress();
                    }
                }
            }
        } catch (SocketException ex) {
            ex.printStackTrace();
        }
        return ip;
    }
}
