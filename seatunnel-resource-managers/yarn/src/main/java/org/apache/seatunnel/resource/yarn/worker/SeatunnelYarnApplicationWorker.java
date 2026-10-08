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

package org.apache.seatunnel.resource.yarn.worker;

import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.config.SeatunnelApplicationConfig;
import org.apache.seatunnel.engine.core.classloader.ApplicationJarPathResolver;
import org.apache.seatunnel.engine.core.classloader.JarPathResolver;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManagerFactory;
import org.apache.seatunnel.resource.yarn.launch.YarnConstants;

import java.nio.file.Paths;

/** Starts one localized application worker without owning YARN application cleanup. */
public final class SeatunnelYarnApplicationWorker {
    private SeatunnelYarnApplicationWorker() {}

    /**
     * Prepares worker configuration and local jar resolution before creating the Engine member.
     * Process termination uses Hazelcast's shutdown hook; staged artifacts belong to the master.
     *
     * @param args cluster name, master address, fixed slot count and master distribution home
     */
    public static void main(String[] args) {
        if (args.length != 4) {
            throw new IllegalArgumentException(
                    "Expected <cluster-name> <master-address> <slots> <master-distribution-home>");
        }
        SeaTunnelConfig config = ConfigProvider.locateAndGetSeaTunnelConfig();
        SeatunnelApplicationConfig.configure(config, args[0], args[1], Integer.parseInt(args[2]));
        config.getHazelcastConfig().getNetworkConfig().setPortAutoIncrement(true);
        config.getHazelcastConfig().setProperty("hazelcast.shutdownhook.enabled", "true");
        config.getHazelcastConfig().setProperty("hazelcast.shutdownhook.policy", "GRACEFUL");
        JarPathResolver jarPathResolver =
                new ApplicationJarPathResolver(
                        args[3],
                        Paths.get(System.getProperty(YarnConstants.SEATUNNEL_HOME_PROPERTY))
                                .toAbsolutePath()
                                .normalize()
                                .toString());
        SeaTunnelServerStarter.createHazelcastInstance(
                config, null, jarPathResolver, new ResourceManagerFactory());
    }
}
