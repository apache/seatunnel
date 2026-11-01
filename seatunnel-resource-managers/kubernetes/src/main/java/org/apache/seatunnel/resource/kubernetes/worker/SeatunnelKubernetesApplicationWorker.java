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

package org.apache.seatunnel.resource.kubernetes.worker;

import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.config.SeatunnelApplicationConfig;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManagerFactory;
import org.apache.seatunnel.resource.kubernetes.config.KubernetesApplicationHaConfig;

import com.hazelcast.cluster.MembershipEvent;
import com.hazelcast.cluster.MembershipListener;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/** Starts one fixed-slot application worker without Kubernetes API access. */
@Slf4j
public final class SeatunnelKubernetesApplicationWorker {

    private SeatunnelKubernetesApplicationWorker() {}

    /**
     * Prepares worker configuration and creates its Engine member. Process termination uses
     * Hazelcast's shutdown hook; application-wide resource cleanup belongs to the master.
     *
     * @param args cluster name, master address and fixed slot count
     */
    public static void main(String[] args) {
        if (args.length != 3 && !(args.length == 6 && "--ha".equals(args[3]))) {
            throw new IllegalArgumentException(
                    "Expected <cluster-name> <master-address> <slots> [--ha <backup-count> <startup-timeout-millis>]");
        }
        SeaTunnelConfig config = ConfigProvider.locateAndGetSeaTunnelConfig();
        boolean ha = args.length == 6;
        if (ha) {
            KubernetesApplicationHaConfig.configure(
                    config,
                    args[0],
                    Arrays.asList(args[1].split(",")),
                    true,
                    Integer.parseInt(args[2]),
                    Integer.parseInt(args[4]));
            awaitMasterService(args[1], Long.parseLong(args[5]));
        } else {
            SeatunnelApplicationConfig.configure(
                    config, args[0], args[1], Integer.parseInt(args[2]));
        }
        config.getHazelcastConfig().getNetworkConfig().setPortAutoIncrement(true);
        config.getHazelcastConfig().setProperty("hazelcast.shutdownhook.enabled", "true");
        config.getHazelcastConfig().setProperty("hazelcast.shutdownhook.policy", "GRACEFUL");
        HazelcastInstanceImpl worker =
                SeaTunnelServerStarter.createHazelcastInstance(
                        config, null, new ResourceManagerFactory());
        if (!ha) {
            worker.getCluster().addMembershipListener(new MasterExitListener());
        }
    }

    static void awaitMasterService(String masterAddress, long timeoutMillis) {
        int separator = masterAddress.lastIndexOf(':');
        if (separator <= 0 || separator != masterAddress.indexOf(':') || timeoutMillis <= 0) {
            throw new IllegalArgumentException(
                    "Expected a master service host:port and a positive startup timeout");
        }
        String host = masterAddress.substring(0, separator);
        int port = Integer.parseInt(masterAddress.substring(separator + 1));
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("Master service port must be between 1 and 65535");
        }
        log.info(
                "Waiting up to {} ms for application master service {}",
                timeoutMillis,
                masterAddress);
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        IOException failure = null;
        while (true) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                throw new IllegalStateException(
                        "Application master service "
                                + masterAddress
                                + " did not become available within "
                                + timeoutMillis
                                + " ms",
                        failure);
            }
            try (Socket socket = new Socket()) {
                socket.connect(
                        new InetSocketAddress(host, port),
                        (int)
                                Math.max(
                                        1,
                                        Math.min(1000, TimeUnit.NANOSECONDS.toMillis(remaining))));
                return;
            } catch (IOException e) {
                failure = e;
            }
            remaining = deadline - System.nanoTime();
            if (remaining > 0) {
                try {
                    TimeUnit.NANOSECONDS.sleep(
                            Math.min(remaining, TimeUnit.MILLISECONDS.toNanos(200)));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(
                            "Interrupted while waiting for application master service "
                                    + masterAddress,
                            e);
                }
            }
        }
    }

    /**
     * Exits this worker when the sole non-lite master member leaves, so an unclean master exit
     * cannot leave lite workers alive and holding their full pod reservations until Job TTL.
     */
    private static final class MasterExitListener implements MembershipListener {
        @Override
        public void memberAdded(MembershipEvent membershipEvent) {}

        @Override
        public void memberRemoved(MembershipEvent membershipEvent) {
            if (!membershipEvent.getMember().isLiteMember()) {
                log.warn(
                        "Application master {} left the cluster; shutting down worker",
                        membershipEvent.getMember().getAddress());
                // Halt instead of exiting gracefully: the master is gone, and running the shutdown
                // hook here would only wait for Hazelcast to drain a lite member.
                Runtime.getRuntime().halt(1);
            }
        }
    }
}
