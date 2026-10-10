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

import com.hazelcast.spi.impl.NodeEngine;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StandaloneResourceManager extends AbstractResourceManager {

    public StandaloneResourceManager(NodeEngine nodeEngine, EngineConfig engineConfig) {
        super(nodeEngine, engineConfig);
    }

    /** Synchronizes existing worker slots without creating external worker processes. */
    @Override
    public synchronized void init() {
        log.info("Init standalone ResourceManager");
        try {
            super.init();
        } catch (Exception e) {
            IllegalStateException initializationFailure =
                    new IllegalStateException(
                            "Could not initialize standalone resource manager", e);
            try {
                close();
            } catch (RuntimeException cleanupFailure) {
                initializationFailure.addSuppressed(cleanupFailure);
            }
            throw initializationFailure;
        }
    }
}
