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
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.SocketException;
import java.util.Enumeration;

/**
 * RocketMQ container for the connector E2E suite.
 *
 * <p>The broker identity is fixed before startup. Updating the name, advertised address and port
 * after the broker has registered leaves a stale identity in the name server, which makes direct
 * sends to {@code broker-a} and consume-offset queries intermittently fail in CI.
 */
public class RocketMqContainer extends GenericContainer<RocketMqContainer> {

    public static final int NAMESRV_PORT = 9876;
    public static final String BROKER_NAME = "broker-a";
    private static final int DEFAULT_BROKER_PERMISSION = 6;
    static final int DEFAULT_TOPIC_QUEUE_NUMS = 4;
    private static final String BROKER_CONF_PATH = "/home/rocketmq/broker.conf";
    private static final int FREE_PORT_ATTEMPTS = 20;

    private final int brokerPort;

    public RocketMqContainer(DockerImageName image) {
        super(image);
        this.brokerPort = findFreeBrokerPort();
        withExposedPorts(NAMESRV_PORT);
        // RocketMQ advertises listenPort verbatim, so the host and container ports must match.
        addFixedExposedPort(brokerPort, brokerPort);
        addFixedExposedPort(brokerPort - 2, brokerPort - 2);
        this.withEnv("JAVA_OPT_EXT", "-Xms512m -Xmx512m");
    }

    @Override
    protected void configure() {
        String brokerConf =
                "brokerClusterName=DefaultCluster\n"
                        + "brokerName="
                        + BROKER_NAME
                        + "\n"
                        + "brokerId=0\n"
                        + "brokerIP1="
                        + getLinuxLocalIp()
                        + "\n"
                        + "listenPort="
                        + brokerPort
                        + "\n"
                        + "autoCreateTopicEnable=true\n"
                        + "defaultTopicQueueNums="
                        + DEFAULT_TOPIC_QUEUE_NUMS
                        + "\n"
                        + "brokerPermission="
                        + DEFAULT_BROKER_PERMISSION
                        + "\n";
        withCopyToContainer(Transferable.of(brokerConf), BROKER_CONF_PATH);
        String command = "#!/bin/bash\n";
        command += "./mqnamesrv &\n";
        command += "./mqbroker -n localhost:" + NAMESRV_PORT + " -c " + BROKER_CONF_PATH;
        withCommand("sh", "-c", command);
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

    private static int findFreeBrokerPort() {
        for (int i = 0; i < FREE_PORT_ATTEMPTS; i++) {
            int port = findFreePort();
            if (port > 2 && isPortFree(port - 2)) {
                return port;
            }
        }
        throw new IllegalStateException(
                "Could not find a free host port pair for the RocketMQ broker");
    }

    private static int findFreePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new IllegalStateException("Could not allocate a free host port", e);
        }
    }

    private static boolean isPortFree(int port) {
        try (ServerSocket socket = new ServerSocket(port)) {
            socket.setReuseAddress(true);
            return true;
        } catch (IOException e) {
            return false;
        }
    }
}
