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

package org.apache.seatunnel.resource.core.client;

import org.apache.seatunnel.engine.common.runtime.DeployType;

import java.util.Map;
import java.util.ServiceLoader;

/** Resolves one unambiguous platform provider from the application class path. */
public final class ApplicationDeployers {
    private ApplicationDeployers() {}
    /**
     * Discovers the unique provider using the current thread's context classloader.
     *
     * @param type requested external platform
     * @param options deployment configuration passed to the provider
     * @return a caller-owned deployer
     * @throws Exception if no unique provider exists or provider initialization fails
     */
    public static ApplicationDeployer create(DeployType type, Map<String, String> options)
            throws Exception {
        ApplicationDeployerFactory selected = null;
        for (ApplicationDeployerFactory factory :
                ServiceLoader.load(ApplicationDeployerFactory.class)) {
            if (factory.getDeployType() == type) {
                if (selected != null) {
                    throw new IllegalStateException(
                            "Multiple application deployment providers for " + type);
                }
                selected = factory;
            }
        }
        if (selected == null) {
            throw new IllegalArgumentException(
                    "No application deployment provider for "
                            + type
                            + "; install its jars under resource-managers/ in the SeaTunnel distribution");
        }
        return selected.create(options);
    }
}
