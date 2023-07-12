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

package org.apache.seatunnel.engine.server.telemetry.metrics;

import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.logging.ILogger;
import io.prometheus.client.Collector;
import org.apache.seatunnel.engine.common.exception.SeaTunnelEngineException;
import org.apache.seatunnel.engine.server.CoordinatorService;
import org.apache.seatunnel.engine.server.SeaTunnelServer;

public abstract class AbstractCollector extends Collector {

    protected ExportsInstance exportsInstance;

    public AbstractCollector(final ExportsInstance exportsInstance) {
        this.exportsInstance = exportsInstance;
    }

    protected Node getNode() {
        return exportsInstance.getNode();
    }

    protected ILogger getLogger(Class clazz) {
        return getNode().getLogger(clazz);
    }

    protected boolean isMaster() {
        return getNode().isMaster();
    }

    protected MemberImpl getLocalMember() {
        return getNode().nodeEngine.getLocalMember();
    }

    protected SeaTunnelServer getServer() {
        return getNode().getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
    }

    protected CoordinatorService getCoordinatorService() {
        if (isMaster()) {
            return getServer().getCoordinatorService();
        } else {
            throw new SeaTunnelEngineException("This is not a master node now.");
        }
    }
}
