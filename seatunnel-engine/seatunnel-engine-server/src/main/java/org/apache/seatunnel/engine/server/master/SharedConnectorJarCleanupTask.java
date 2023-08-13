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

package org.apache.seatunnel.engine.server.master;

import org.apache.seatunnel.engine.core.job.ConnectorJarIdentifier;
import org.apache.seatunnel.engine.core.job.RefCount;

import com.hazelcast.logging.ILogger;
import com.hazelcast.map.IMap;

import java.util.Iterator;
import java.util.Map;
import java.util.TimerTask;
import java.util.function.Consumer;

import static org.apache.curator.shaded.com.google.common.base.Preconditions.checkNotNull;

/*
Cleanup task for shared connector jar package.
 */
public class SharedConnectorJarCleanupTask extends TimerTask {

    private final ILogger LOGGER;

    private final Consumer<ConnectorJarIdentifier> cleanupCallback;

    private final IMap<ConnectorJarIdentifier, RefCount> connectorJarRefCounters;

    public SharedConnectorJarCleanupTask(
            ILogger LOGGER,
            Consumer<ConnectorJarIdentifier> cleanupCallback,
            IMap<ConnectorJarIdentifier, RefCount> connectorJarRefCounters) {
        this.LOGGER = checkNotNull(LOGGER);
        this.cleanupCallback = checkNotNull(cleanupCallback);
        this.connectorJarRefCounters = checkNotNull(connectorJarRefCounters);
    }

    /** Cleans up connectorJars which are not referenced anymore. */
    @Override
    public void run() {
        synchronized (connectorJarRefCounters) {
            Iterator<Map.Entry<ConnectorJarIdentifier, RefCount>> iterator =
                    connectorJarRefCounters.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<ConnectorJarIdentifier, RefCount> entry = iterator.next();
                if (entry.getValue().getReferences() <= 0) {
                    ConnectorJarIdentifier connectorJarIdentifier = entry.getKey();
                    cleanupCallback.accept(connectorJarIdentifier);
                }
            }
        }
    }
}
