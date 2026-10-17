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

package org.apache.seatunnel.engine.server.joiner;

import org.apache.seatunnel.engine.server.TestUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.cluster.Address;
import com.hazelcast.config.Config;
import com.hazelcast.config.DiscoveryConfig;
import com.hazelcast.config.DiscoveryStrategyConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.config.properties.PropertyDefinition;
import com.hazelcast.instance.impl.DefaultNodeContext;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.instance.impl.HazelcastInstanceProxy;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.cluster.Joiner;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.DiscoveryStrategy;
import com.hazelcast.spi.discovery.DiscoveryStrategyFactory;
import com.hazelcast.spi.discovery.SimpleDiscoveryNode;

import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Verifies that the discovery-based lite-member joiner ({@link LiteNodeDropOutDiscoveryJoiner})
 * inherits the TCP/IP lite-member retry fix from {@link LiteNodeDropOutTcpIpJoiner}.
 *
 * <p>Note on scope: the TCP/IP fix clears the <em>temporary</em> blacklist of candidate addresses
 * so they are retried. For discovery this only helps when the initial discovery already returned
 * the master address (which is temporarily unreachable). If the initial discovery returns an empty
 * list, a later-starting master is <em>not</em> picked up because {@link
 * LiteNodeDropOutDiscoveryJoiner#getPossibleAddressesForInitialJoin()} polls only until {@code
 * hazelcast.wait.seconds.before.join} and then returns an empty collection, and the retry loop does
 * not re-run discovery. Refreshing discovery within the overall join deadline is intentionally left
 * out of this focused fix.
 */
class LiteNodeDropOutDiscoveryJoinerTest {

    private static final String HOST = "127.0.0.1";

    /**
     * A discovery-based worker (lite member) starts before the master. The discovery strategy
     * already returns the master address before the master port is ready, so the worker temporarily
     * blacklists it and must keep retrying (inherited from the TCP/IP fix) until the master starts
     * and the worker joins.
     */
    @Test
    void discoveryWorkerStartedBeforeMasterShouldEventuallyJoin() throws Exception {
        String clusterName =
                TestUtils.getClusterName("LiteNodeDropOutDiscoveryJoinerTest_workerJoin");
        int masterPort = TestUtils.getAvailablePort();
        int workerPort = TestUtils.getAvailablePort();

        Config workerConfig = newDiscoveryConfig(clusterName, workerPort, masterPort);
        workerConfig.setLiteMember(true);
        workerConfig.setProperty("hazelcast.max.join.seconds", "60");
        // Keep the initial discovery poll window short; the strategy returns a node immediately.
        workerConfig.setProperty("hazelcast.wait.seconds.before.join", "1");

        // Fires once the worker has actually blacklisted the not-yet-started master address.
        CountDownLatch blacklistLatch = new CountDownLatch(1);

        AtomicReference<HazelcastInstanceImpl> workerRef = new AtomicReference<>();
        AtomicReference<Throwable> workerError = new AtomicReference<>();
        Thread workerThread =
                new Thread(
                        () -> {
                            try {
                                workerRef.set(startNode(workerConfig, blacklistLatch));
                            } catch (Throwable t) {
                                workerError.set(t);
                            }
                        },
                        "test-discovery-worker-started-before-master");
        workerThread.start();

        HazelcastInstanceImpl master = null;
        try {
            // Wait until the worker has provably blacklisted the not-yet-started master address.
            Assertions.assertTrue(
                    blacklistLatch.await(30, SECONDS),
                    "worker should have blacklisted the not-yet-started master address");

            Config masterConfig = newTcpIpConfig(clusterName, masterPort);
            master = startNode(masterConfig, null);

            workerThread.join(SECONDS.toMillis(60));
            Assertions.assertFalse(workerThread.isAlive(), "worker should have finished joining");
            Assertions.assertNull(workerError.get(), "worker should not fail to start");

            HazelcastInstanceImpl worker = workerRef.get();
            Assertions.assertNotNull(worker);
            Assertions.assertTrue(worker.node.getClusterService().isJoined());
            Assertions.assertFalse(worker.node.isMaster(), "a worker must never become master");
            Assertions.assertTrue(master.node.isMaster());
        } finally {
            workerThread.interrupt();
            try {
                workerThread.join(SECONDS.toMillis(5));
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
            if (workerRef.get() != null) {
                workerRef.get().shutdown();
            }
            if (master != null) {
                master.shutdown();
            }
            // Force-stop any still-starting worker node (e.g. when a timeout assertion failed
            // before workerRef was set) so it cannot leak into later tests in the same JVM.
            HazelcastInstanceFactory.shutdownAll();
        }
    }

    private static HazelcastInstanceImpl startNode(Config config, CountDownLatch blacklistLatch) {
        String instanceName = HazelcastInstanceFactory.createInstanceName(config);
        HazelcastInstanceProxy proxy =
                (HazelcastInstanceProxy)
                        HazelcastInstanceFactory.newHazelcastInstance(
                                config,
                                instanceName,
                                new LiteNodeDropOutDiscoveryJoinerContext(blacklistLatch));
        return proxy.getOriginal();
    }

    private static Config newDiscoveryConfig(String clusterName, int workerPort, int masterPort)
            throws UnknownHostException {
        Config config = new Config();
        config.setClusterName(clusterName);
        config.getNetworkConfig().setPort(workerPort);
        config.getNetworkConfig().setPortAutoIncrement(false);
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
        DiscoveryConfig discoveryConfig = config.getNetworkConfig().getJoin().getDiscoveryConfig();
        discoveryConfig.addDiscoveryStrategyConfig(
                new DiscoveryStrategyConfig(
                        new StaticDiscoveryStrategyFactory(new Address(HOST, masterPort))));
        return config;
    }

    private static Config newTcpIpConfig(String clusterName, int port) {
        Config config = new Config();
        config.setClusterName(clusterName);
        config.getNetworkConfig().setPort(port);
        config.getNetworkConfig().setPortAutoIncrement(false);
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        TcpIpConfig tcpIpConfig = config.getNetworkConfig().getJoin().getTcpIpConfig();
        tcpIpConfig.setEnabled(true);
        tcpIpConfig.setConnectionTimeoutSeconds(1);
        return config;
    }

    /** A discovery joiner that counts down the given latch when an address is blacklisted. */
    private static class CountingBlacklistDiscoveryJoiner extends LiteNodeDropOutDiscoveryJoiner {

        private final CountDownLatch blacklistLatch;

        CountingBlacklistDiscoveryJoiner(Node node, CountDownLatch blacklistLatch) {
            super(node);
            this.blacklistLatch = blacklistLatch;
        }

        @Override
        public void blacklist(Address address, boolean permanent) {
            super.blacklist(address, permanent);
            if (blacklistLatch != null) {
                blacklistLatch.countDown();
            }
        }
    }

    /** Node context that selects the discovery joiner for lite members and TCP/IP otherwise. */
    private static class LiteNodeDropOutDiscoveryJoinerContext extends DefaultNodeContext {

        private final CountDownLatch blacklistLatch;

        LiteNodeDropOutDiscoveryJoinerContext(CountDownLatch blacklistLatch) {
            this.blacklistLatch = blacklistLatch;
        }

        @Override
        public Joiner createJoiner(Node node) {
            return node.isLiteMember()
                    ? new CountingBlacklistDiscoveryJoiner(node, blacklistLatch)
                    : new LiteNodeDropOutTcpIpJoiner(node);
        }
    }

    /** Discovery strategy that always returns the fixed master address. */
    private static final class StaticDiscoveryStrategy implements DiscoveryStrategy {

        private final Iterable<DiscoveryNode> nodes;

        private StaticDiscoveryStrategy(Address address) {
            this.nodes = Collections.singleton(new SimpleDiscoveryNode(address));
        }

        @Override
        public void start() {}

        @Override
        public Iterable<DiscoveryNode> discoverNodes() {
            return nodes;
        }

        @Override
        public void destroy() {}

        @Override
        public Map<String, String> discoverLocalMetadata() {
            return Collections.emptyMap();
        }
    }

    private static final class StaticDiscoveryStrategyFactory implements DiscoveryStrategyFactory {

        private final Address address;

        private StaticDiscoveryStrategyFactory(Address address) {
            this.address = address;
        }

        @Override
        public Class<? extends DiscoveryStrategy> getDiscoveryStrategyType() {
            return StaticDiscoveryStrategy.class;
        }

        @Override
        public DiscoveryStrategy newDiscoveryStrategy(
                DiscoveryNode discoveryNode, ILogger logger, Map<String, Comparable> properties) {
            return new StaticDiscoveryStrategy(address);
        }

        @Override
        public Collection<PropertyDefinition> getConfigurationProperties() {
            return Collections.emptyList();
        }
    }
}
