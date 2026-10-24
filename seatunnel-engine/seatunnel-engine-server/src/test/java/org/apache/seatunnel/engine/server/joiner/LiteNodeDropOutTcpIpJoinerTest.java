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
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.instance.impl.DefaultNodeContext;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.instance.impl.HazelcastInstanceProxy;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.cluster.Joiner;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Verifies the lite-member (worker) join behavior of {@link LiteNodeDropOutTcpIpJoiner}.
 *
 * <p>A worker is a Hazelcast lite member and therefore can never assume the master role. The
 * regression being guarded here is: when a worker starts before the master, connecting to the
 * candidate master addresses fails and they are temporarily blacklisted. Without the retry fix the
 * worker would never retry those addresses and would deadlock until {@code
 * hazelcast.max.join.seconds} elapses.
 */
class LiteNodeDropOutTcpIpJoinerTest {

    private static final String HOST = "127.0.0.1";

    private static Config newConfig(String clusterName, int port, String... members) {
        Config config = new Config();
        config.setClusterName(clusterName);
        config.getNetworkConfig().setPort(port);
        config.getNetworkConfig().setPortAutoIncrement(false);
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        TcpIpConfig tcpIpConfig = config.getNetworkConfig().getJoin().getTcpIpConfig();
        tcpIpConfig.setEnabled(true);
        // Keep the connection timeout small so that a failed connection attempt against a
        // not-yet-started master is detected and blacklisted quickly, making the test
        // deterministic.
        tcpIpConfig.setConnectionTimeoutSeconds(1);
        tcpIpConfig.setMembers(Arrays.asList(members));
        return config;
    }

    private static HazelcastInstanceImpl startNode(Config config, CountDownLatch blacklistLatch) {
        String instanceName = HazelcastInstanceFactory.createInstanceName(config);
        HazelcastInstanceProxy proxy =
                (HazelcastInstanceProxy)
                        HazelcastInstanceFactory.newHazelcastInstance(
                                config,
                                instanceName,
                                new LiteNodeDropOutJoinerContext(blacklistLatch));
        return proxy.getOriginal();
    }

    /**
     * A worker (lite member) starts before the master. The worker initially fails to connect to the
     * not-yet-started master and temporarily blacklists its address, then retries. Once the master
     * is started (before the join timeout), the worker must join the cluster and must never become
     * master itself.
     *
     * <p>The test starts the master only after the worker has been observed to blacklist the master
     * address (signaled via a latch), guaranteeing the temporary-blacklist retry path is exercised
     * rather than a plain first-attempt join.
     */
    @Test
    void workerStartedBeforeMasterShouldEventuallyJoin() throws Exception {
        String clusterName = TestUtils.getClusterName("LiteNodeDropOutTcpIpJoinerTest_workerJoin");
        int masterPort = TestUtils.getAvailablePort();
        int workerPort = TestUtils.getAvailablePort();

        Config workerConfig = newConfig(clusterName, workerPort, HOST + ":" + masterPort);
        workerConfig.setLiteMember(true);
        // Generous join window; the worker is expected to join well before this expires.
        workerConfig.setProperty("hazelcast.max.join.seconds", "60");

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
                        "test-worker-started-before-master");
        workerThread.start();

        HazelcastInstanceImpl master = null;
        try {
            // Wait until the worker has provably blacklisted the not-yet-started master address.
            Assertions.assertTrue(
                    blacklistLatch.await(30, SECONDS),
                    "worker should have blacklisted the not-yet-started master address");

            Config masterConfig = newConfig(clusterName, masterPort);
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
     * A worker (lite member) with no master available must time out and must never assume the
     * master role. If the worker incorrectly self-elected, {@code startNode} would return
     * successfully instead of throwing.
     */
    @Test
    void workerWithoutMasterShouldTimeoutAndNeverBecomeMaster() {
        String clusterName =
                TestUtils.getClusterName("LiteNodeDropOutTcpIpJoinerTest_workerTimeout");
        int masterPort = TestUtils.getAvailablePort(); // never started
        int workerPort = TestUtils.getAvailablePort();

        Config workerConfig = newConfig(clusterName, workerPort, HOST + ":" + masterPort);
        workerConfig.setLiteMember(true);
        workerConfig.setProperty("hazelcast.max.join.seconds", "5");

        Assertions.assertThrows(IllegalStateException.class, () -> startNode(workerConfig, null));
    }

    /**
     * The non-lite member path must remain unchanged. A non-lite member whose candidate addresses
     * are all blacklisted (for example, a dead target address) must still assume the master role
     * instead of entering the lite-member retry loop.
     */
    @Test
    void nonLiteMemberShouldAssumeMasterWhenAllAddressesBlacklisted() {
        String clusterName =
                TestUtils.getClusterName("LiteNodeDropOutTcpIpJoinerTest_nonLiteSelfElect");
        int deadTargetPort = TestUtils.getAvailablePort(); // never started
        int memberPort = TestUtils.getAvailablePort();

        Config config = newConfig(clusterName, memberPort, HOST + ":" + deadTargetPort);
        config.setProperty("hazelcast.max.join.seconds", "30");

        HazelcastInstanceImpl instance = startNode(config, null);
        try {
            Assertions.assertTrue(instance.node.getClusterService().isJoined());
            Assertions.assertTrue(instance.node.isMaster());
        } finally {
            instance.shutdown();
        }
    }

    /** A reachable master must stay excluded when its address is permanently blacklisted. */
    @Test
    void retryMustNotClearAPermanentBlacklistEvenWhenTheMasterIsReachable() throws Exception {
        String clusterName = TestUtils.getClusterName("LiteNodeDropOutTcpIpJoinerTest_permanent");
        int masterPort = TestUtils.getAvailablePort();
        int workerPort = TestUtils.getAvailablePort();
        Address masterAddress = new Address(HOST, masterPort);
        HazelcastInstanceImpl master = startNode(newConfig(clusterName, masterPort), null);
        AtomicReference<HazelcastInstanceImpl> unexpectedWorker = new AtomicReference<>();
        try {
            Config workerConfig = newConfig(clusterName, workerPort, HOST + ":" + masterPort);
            workerConfig.setLiteMember(true);
            workerConfig.setProperty("hazelcast.max.join.seconds", "15");
            Assertions.assertThrows(
                    IllegalStateException.class,
                    () -> {
                        String instanceName =
                                HazelcastInstanceFactory.createInstanceName(workerConfig);
                        HazelcastInstanceProxy proxy =
                                (HazelcastInstanceProxy)
                                        HazelcastInstanceFactory.newHazelcastInstance(
                                                workerConfig,
                                                instanceName,
                                                new DefaultNodeContext() {
                                                    @Override
                                                    public Joiner createJoiner(Node node) {
                                                        return new LiteNodeDropOutTcpIpJoiner(
                                                                node) {
                                                            @Override
                                                            public void doJoin() {
                                                                // AbstractJoiner.join clears
                                                                // old entries before doJoin, so
                                                                // inject the permanent exclusion
                                                                // here.
                                                                blacklist(masterAddress, true);
                                                                super.doJoin();
                                                            }
                                                        };
                                                    }
                                                });
                        unexpectedWorker.set(proxy.getOriginal());
                    });
            Assertions.assertTrue(master.node.getClusterService().isJoined());
            Assertions.assertTrue(master.node.isMaster());
            Assertions.assertEquals(1, master.node.getClusterService().getMembers().size());
        } finally {
            if (unexpectedWorker.get() != null) {
                unexpectedWorker.get().shutdown();
            }
            master.shutdown();
        }
    }

    /**
     * A joiner that counts down the given latch whenever an address is blacklisted, so tests can
     * deterministically wait for the worker to reach the temporary-blacklist state.
     */
    private static class CountingBlacklistJoiner extends LiteNodeDropOutTcpIpJoiner {

        private final CountDownLatch blacklistLatch;

        CountingBlacklistJoiner(Node node, CountDownLatch blacklistLatch) {
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

    /**
     * Minimal {@link com.hazelcast.instance.impl.NodeContext} that wires in the joiner under test.
     */
    private static class LiteNodeDropOutJoinerContext extends DefaultNodeContext {

        private final CountDownLatch blacklistLatch;

        LiteNodeDropOutJoinerContext(CountDownLatch blacklistLatch) {
            this.blacklistLatch = blacklistLatch;
        }

        @Override
        public Joiner createJoiner(Node node) {
            return new CountingBlacklistJoiner(node, blacklistLatch);
        }
    }
}
