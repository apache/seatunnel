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

package org.apache.seatunnel.engine.server.resourcemanager;

import org.apache.seatunnel.engine.common.config.EngineConfig;
import org.apache.seatunnel.engine.common.runtime.DeployType;
import org.apache.seatunnel.engine.server.resourcemanager.thirdparty.kubernetes.KubernetesResourceManager;
import org.apache.seatunnel.engine.server.resourcemanager.thirdparty.yarn.YarnResourceManager;

import com.hazelcast.spi.impl.NodeEngine;

public class ResourceManagerFactory {

    private final NodeEngine nodeEngine;

    private final EngineConfig engineConfig;

    public ResourceManagerFactory(NodeEngine nodeEngine, EngineConfig engineConfig) {
        this.nodeEngine = nodeEngine;
        this.engineConfig = engineConfig;
    }

    public ResourceManager getResourceManager(DeployType type) {
        if (DeployType.STANDALONE.equals(type)) {
            return new StandaloneResourceManager(nodeEngine, engineConfig);
        } else if (DeployType.KUBERNETES.equals(type)) {
            return new KubernetesResourceManager(nodeEngine, engineConfig);
        } else if (DeployType.YARN.equals(type)) {
            return new YarnResourceManager(nodeEngine, engineConfig);
        } else {
            throw new UnsupportedDeployTypeException(type);
        }
    }

    public ResourceManager getResourceManager() {
        return this.getResourceManager(DeployType.STANDALONE);
    }
}
