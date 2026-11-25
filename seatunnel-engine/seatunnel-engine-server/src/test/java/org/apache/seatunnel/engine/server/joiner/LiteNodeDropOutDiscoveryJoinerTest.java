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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Verifies that the discovery-based lite-member joiner ({@link LiteNodeDropOutDiscoveryJoiner})
 * inherits the TCP/IP lite-member retry fix from {@link LiteNodeDropOutTcpIpJoiner}, including
 * refreshing an initially-empty discovery result within the overall join deadline.
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

    /**
     * A discovery-based worker (lite member) starts before the master, but discovery returns no
     * candidate until after the initial discovery window. The worker must keep refreshing discovery
     * within the join deadline and join once the master becomes discoverable, without ever becoming
     * master itself.
     *
     * <p>The master is made discoverable only after the initial discovery window ({@code
     * hazelcast.wait.seconds.before.join}=1s) has elapsed, so the empty-list refresh path (not the
     * initial-discovery path) is exercised.
     */
    @Test
    void discoveryWorkerStartedWithEmptyDiscoveryShouldEventuallyJoin() throws Exception {
        String clusterName =
                TestUtils.getClusterName("LiteNodeDropOutDiscoveryJoinerTest_emptyJoin");
        int masterPort = TestUtils.getAvailablePort();
        int workerPort = TestUtils.getAvailablePort();

        // Discovery returns no candidates until this latch is counted down (i.e. the master is up).
        CountDownLatch masterReady = new CountDownLatch(1);
        AtomicInteger discoverCount = new AtomicInteger();

        Config workerConfig =
                newEmptyDiscoveryConfig(
                        clusterName, workerPort, masterPort, masterReady, discoverCount);
        workerConfig.setLiteMember(true);
        workerConfig.setProperty("hazelcast.max.join.seconds", "60");
        workerConfig.setProperty("hazelcast.wait.seconds.before.join", "1");

        AtomicReference<HazelcastInstanceImpl> workerRef = new AtomicReference<>();
        AtomicReference<Throwable> workerError = new AtomicReference<>();
        Thread workerThread =
                new Thread(
                        () -> {
                            try {
                                workerRef.set(startNode(workerConfig, null));
                            } catch (Throwable t) {
                                workerError.set(t);
                            }
                        },
                        "test-discovery-empty-join");
        workerThread.start();

        HazelcastInstanceImpl master = null;
        try {
            // Wait until the worker has polled discovery enough times to be past the initial
            // discovery window (hazelcast.wait.seconds.before.join=1s) and into the empty-list
            // refresh loop. The initial window polls roughly 7 times with the backoff idle
            // strategy, so 10 polls guarantee the refresh path is exercised. Polling the counter
            // instead of a fixed sleep keeps this deterministic even on slow runners.
            long pollDeadline = System.currentTimeMillis() + SECONDS.toMillis(30);
            while (discoverCount.get() < 10 && System.currentTimeMillis() < pollDeadline) {
                Thread.sleep(100);
            }
            Assertions.assertTrue(
                    discoverCount.get() >= 10,
                    "worker should have polled discovery past the initial window");

            // Record how many times discovery has been polled so far, then make the master
            // discoverable and verify the worker polls discovery again (not just joins).
            int discoveriesBeforeMaster = discoverCount.get();

            master = startNode(newTcpIpConfig(clusterName, masterPort), null);
            masterReady.countDown();

            workerThread.join(SECONDS.toMillis(60));
            Assertions.assertFalse(workerThread.isAlive(), "worker should have finished joining");
            Assertions.assertNull(workerError.get(), "worker should not fail to start");
            Assertions.assertTrue(
                    discoverCount.get() > discoveriesBeforeMaster,
                    "worker should have re-polled discovery after the master became discoverable");

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
            HazelcastInstanceFactory.shutdownAll();
        }
    }

    /**
     * Interrupting a discovery-based worker whose discovery keeps returning no candidates must stop
     * the retry loop promptly (rather than spinning until {@code hazelcast.max.join.seconds}), and
     * the worker must not join.
     */
    @Test
    void interruptedWorkerWithEmptyCandidatesShouldStopRetrying() throws Exception {
        String clusterName =
                TestUtils.getClusterName("LiteNodeDropOutDiscoveryJoinerTest_interruptEmpty");
        int masterPort = TestUtils.getAvailablePort(); // never published
        int workerPort = TestUtils.getAvailablePort();

        CountDownLatch masterReady = new CountDownLatch(1); // never counted down
        AtomicInteger discoverCount = new AtomicInteger();

        Config workerConfig =
                newEmptyDiscoveryConfig(
                        clusterName, workerPort, masterPort, masterReady, discoverCount);
        workerConfig.setLiteMember(true);
        workerConfig.setProperty("hazelcast.max.join.seconds", "60");
        workerConfig.setProperty("hazelcast.wait.seconds.before.join", "1");

        AtomicReference<HazelcastInstanceImpl> workerRef = new AtomicReference<>();
        Thread workerThread =
                new Thread(
                        () -> {
                            try {
                                workerRef.set(startNode(workerConfig, null));
                            } catch (Throwable ignored) {
                                // The join attempt is expected to fail after interruption.
                            }
                        },
                        "test-interrupt-empty-candidates");
        workerThread.start();

        try {
            // Wait until the worker has passed the initial discovery window and is polling
            // discovery from the empty-list refresh loop.
            long pollDeadline = System.currentTimeMillis() + SECONDS.toMillis(30);
            while (discoverCount.get() < 10 && System.currentTimeMillis() < pollDeadline) {
                Thread.sleep(100);
            }
            Assertions.assertTrue(
                    discoverCount.get() >= 10, "worker should have entered the refresh loop");

            workerThread.interrupt();
            workerThread.join(SECONDS.toMillis(5));
            Assertions.assertFalse(
                    workerThread.isAlive(),
                    "worker should stop retrying promptly after interruption");
            Assertions.assertNull(workerRef.get(), "worker should not have joined");
        } finally {
            workerThread.interrupt();
            if (workerRef.get() != null) {
                workerRef.get().shutdown();
            }
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

    private static Config newEmptyDiscoveryConfig(
            String clusterName,
            int workerPort,
            int masterPort,
            CountDownLatch masterReady,
            AtomicInteger discoverCount)
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
                        new InitiallyEmptyDiscoveryStrategyFactory(
                                new Address(HOST, masterPort), masterReady, discoverCount)));
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

    /**
     * Discovery strategy that returns no nodes until the given latch is counted down, then returns
     * the fixed master address. Models a master that is not published to discovery until after the
     * worker has started.
     */
    private static final class InitiallyEmptyDiscoveryStrategy implements DiscoveryStrategy {

        private final Address address;
        private final CountDownLatch masterReady;
        private final AtomicInteger discoverCount;

        private InitiallyEmptyDiscoveryStrategy(
                Address address, CountDownLatch masterReady, AtomicInteger discoverCount) {
            this.address = address;
            this.masterReady = masterReady;
            this.discoverCount = discoverCount;
        }

        @Override
        public void start() {}

        @Override
        public Iterable<DiscoveryNode> discoverNodes() {
            discoverCount.incrementAndGet();
            if (masterReady.getCount() > 0) {
                return Collections.emptyList();
            }
            return Collections.singleton(new SimpleDiscoveryNode(address));
        }

        @Override
        public void destroy() {}

        @Override
        public Map<String, String> discoverLocalMetadata() {
            return Collections.emptyMap();
        }
    }

    private static final class InitiallyEmptyDiscoveryStrategyFactory
            implements DiscoveryStrategyFactory {

        private final Address address;
        private final CountDownLatch masterReady;
        private final AtomicInteger discoverCount;

        private InitiallyEmptyDiscoveryStrategyFactory(
                Address address, CountDownLatch masterReady, AtomicInteger discoverCount) {
            this.address = address;
            this.masterReady = masterReady;
            this.discoverCount = discoverCount;
        }

        @Override
        public Class<? extends DiscoveryStrategy> getDiscoveryStrategyType() {
            return InitiallyEmptyDiscoveryStrategy.class;
        }

        @Override
        public DiscoveryStrategy newDiscoveryStrategy(
                DiscoveryNode discoveryNode, ILogger logger, Map<String, Comparable> properties) {
            return new InitiallyEmptyDiscoveryStrategy(address, masterReady, discoverCount);
        }

        @Override
        public Collection<PropertyDefinition> getConfigurationProperties() {
            return Collections.emptyList();
        }
    }
}
