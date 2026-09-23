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

package org.apache.seatunnel.core.starter.seatunnel.application;

import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.core.classloader.JarPathResolver;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.resource.core.classloader.ApplicationJarPathResolver;
import org.apache.seatunnel.resource.core.config.ApplicationClusterConfig;

import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.MembershipEvent;
import com.hazelcast.cluster.MembershipListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;

import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/** Worker process entrypoint for an application-owned, fixed-size cluster. */
public final class ApplicationWorker {
    private ApplicationWorker() {}

    /**
     * Starts a worker process and waits until it is stopped or its only master leaves the cluster.
     *
     * <p>The optional fourth argument enables distribution-root jar localization for containers
     * whose filesystem paths differ from the master. Shutdown hooks close the owned worker; this
     * entrypoint neither starts an independent cluster nor replaces a failed master.
     *
     * @param args cluster name, master host:port, slot count, and optional master distribution root
     * @throws Exception if arguments, configuration, cluster joining, or interrupted waiting fail
     */
    public static void main(String[] args) throws Exception {
        if (args.length != 3 && args.length != 4) {
            throw new IllegalArgumentException(
                    "Expected <cluster-name> <master-address> <slots> [master-distribution-home]");
        }
        SeaTunnelConfig config = ConfigProvider.locateAndGetSeaTunnelConfig();
        JarPathResolver jarPathResolver = JarPathResolver.identity();
        if (args.length == 4) {
            jarPathResolver =
                    new ApplicationJarPathResolver(
                            args[3],
                            Paths.get(System.getProperty("seatunnel.home"))
                                    .toAbsolutePath()
                                    .normalize()
                                    .toString());
        }
        HazelcastInstance worker =
                start(args[0], args[1], Integer.parseInt(args[2]), config, jarPathResolver);
        CountDownLatch stopped = new CountDownLatch(1);
        worker.getLifecycleService()
                .addLifecycleListener(
                        event -> {
                            if (event.getState() == LifecycleEvent.LifecycleState.SHUTDOWN) {
                                stopped.countDown();
                            }
                        });
        Thread hook = new Thread(worker::shutdown, "seatunnel-application-worker-shutdown");
        Runtime.getRuntime().addShutdownHook(hook);
        try {
            if (worker.getLifecycleService().isRunning()) {
                stopped.await();
            }
        } finally {
            worker.shutdown();
            try {
                Runtime.getRuntime().removeShutdownHook(hook);
            } catch (IllegalStateException ignored) {
                // The VM has already started executing shutdown hooks.
            }
        }
    }

    /**
     * Starts one lite worker that must join the supplied application master.
     *
     * <p>The caller owns the returned instance and must shut it down. A membership callback also
     * asynchronously stops the instance when the non-lite master disappears; shutdown is never
     * performed directly on the membership event thread.
     *
     * @param clusterName unique cluster name assigned by the application runtime
     * @param masterAddress reachable master host:port; must not be null or blank
     * @param slots positive fixed number of task slots on this worker
     * @return the started worker instance after its initial cluster join
     * @throws IllegalArgumentException if the cluster name, endpoint, or slot count is invalid
     */
    public static HazelcastInstance start(String clusterName, String masterAddress, int slots) {
        return start(
                clusterName, masterAddress, slots, ConfigProvider.locateAndGetSeaTunnelConfig());
    }

    static HazelcastInstance start(
            String clusterName, String masterAddress, int slots, SeaTunnelConfig config) {
        return start(clusterName, masterAddress, slots, config, JarPathResolver.identity());
    }

    /**
     * Starts an application worker with explicit node-local artifact resolution.
     *
     * @param clusterName unique application cluster identity
     * @param masterAddress required application master endpoint
     * @param slots fixed worker task-slot count
     * @param config worker Engine configuration, configured before node creation
     * @param jarPathResolver stable local artifact resolver passed directly to the Engine
     * @return joined worker instance owned by the caller and stopped on master loss
     * @throws IllegalArgumentException if cluster identity or master endpoint is invalid
     */
    static HazelcastInstance start(
            String clusterName,
            String masterAddress,
            int slots,
            SeaTunnelConfig config,
            JarPathResolver jarPathResolver) {
        if (masterAddress == null) {
            throw new IllegalArgumentException("An application master address is required");
        }
        ApplicationClusterConfig.configure(config, clusterName, masterAddress, slots);
        config.getHazelcastConfig().getNetworkConfig().setPortAutoIncrement(true);
        HazelcastInstance worker =
                SeaTunnelServerStarter.createWorkerHazelcastInstance(config, jarPathResolver);
        AtomicBoolean stopping = new AtomicBoolean();
        Runnable stop =
                () -> {
                    if (stopping.compareAndSet(false, true)) {
                        // Membership callbacks run on an engine thread, which shutdown itself must
                        // join.
                        Thread shutdown =
                                new Thread(worker::shutdown, "seatunnel-orphan-worker-shutdown");
                        shutdown.setDaemon(true);
                        shutdown.start();
                    }
                };
        worker.getCluster()
                .addMembershipListener(
                        new MembershipListener() {
                            @Override
                            public void memberAdded(MembershipEvent event) {}

                            @Override
                            public void memberRemoved(MembershipEvent event) {
                                if (!event.getMember().isLiteMember()) {
                                    stop.run();
                                }
                            }
                        });
        if (worker.getCluster().getMembers().stream().allMatch(Member::isLiteMember)) {
            stop.run();
        }
        return worker;
    }
}
