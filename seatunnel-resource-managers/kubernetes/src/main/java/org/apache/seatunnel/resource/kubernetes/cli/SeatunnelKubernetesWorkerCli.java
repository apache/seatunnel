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

package org.apache.seatunnel.resource.kubernetes.cli;

import org.apache.seatunnel.engine.common.config.ApplicationClusterConfig;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.core.classloader.JarPathResolver;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManagerFactory;

/** Starts one fixed-slot application worker without Kubernetes API access. */
public final class SeatunnelKubernetesWorkerCli {
    private SeatunnelKubernetesWorkerCli() {}

    /**
     * Prepares worker configuration and creates its Engine member. Process termination uses
     * Hazelcast's shutdown hook; application-wide resource cleanup belongs to the master.
     *
     * @param args cluster name, master address and fixed slot count
     */
    public static void main(String[] args) {
        if (args.length != 3) {
            throw new IllegalArgumentException("Expected <cluster-name> <master-address> <slots>");
        }
        SeaTunnelConfig config = ConfigProvider.locateAndGetSeaTunnelConfig();
        ApplicationClusterConfig.configure(config, args[0], args[1], Integer.parseInt(args[2]));
        config.getHazelcastConfig().getNetworkConfig().setPortAutoIncrement(true);
        config.getHazelcastConfig().setProperty("hazelcast.shutdownhook.enabled", "true");
        config.getHazelcastConfig().setProperty("hazelcast.shutdownhook.policy", "GRACEFUL");
        SeaTunnelServerStarter.createHazelcastInstance(
                config, null, JarPathResolver.identity(), new ResourceManagerFactory());
    }
}
