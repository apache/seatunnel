/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.e2e.connector.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import lombok.Getter;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.Arrays;

import static org.apache.seatunnel.e2e.common.container.TestContainer.NETWORK;

public class HbaseContainer extends GenericContainer<HbaseContainer> {
    private static final int ZOOKEEPER_PORT = 2181;
    private static final int MASTER_PORT = 16000;
    private static final int REGION_PORT = 16020;
    private static final String HOST = "hbase_e2e";
    private static final int STARTUP_TIMEOUT = 5;
    @Getter private Configuration configuration;

    public HbaseContainer(DockerImageName dockerImageName) throws UnknownHostException {
        super(dockerImageName);
        configuration = HBaseConfiguration.create();
        addExposedPort(MASTER_PORT);
        addExposedPort(REGION_PORT);
        addExposedPort(ZOOKEEPER_PORT);
        String hostname = InetAddress.getLocalHost().getHostName();

        withNetwork(NETWORK);
        withNetworkAliases(HOST);
        withCreateContainerCmdModifier(cmd -> cmd.withHostName(hostname));
        withStartupTimeout(Duration.ofMinutes(STARTUP_TIMEOUT));
        withEnv("HBASE_MASTER_PORT", String.valueOf(MASTER_PORT));
        withEnv("HBASE_REGION_PORT", String.valueOf(REGION_PORT));
        withEnv("HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT", String.valueOf(ZOOKEEPER_PORT));
        withEnv("HBASE_ZOOKEEPER_QUORUM", HOST);

        setPortBindings(
                Arrays.asList(
                        String.format("%s:%s", MASTER_PORT, MASTER_PORT),
                        String.format("%s:%s", REGION_PORT, REGION_PORT),
                        String.format("%s:%s", ZOOKEEPER_PORT, ZOOKEEPER_PORT)));
    }

    public String getZookeeperQuorum() {
        String host = null;
        try {
            host = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
        return String.format("%s:%s", host, getMappedPort(ZOOKEEPER_PORT));
    }
}
