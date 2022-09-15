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

package org.apache.seatunnel.engine.server;

import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;

import com.hazelcast.config.Config;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.instance.impl.HazelcastInstanceProxy;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngine;
import org.junit.After;
import org.junit.Before;

public abstract class AbstractSeaTunnelServerTest {

    protected SeaTunnelServer server;

    protected NodeEngine nodeEngine;

    protected HazelcastInstanceImpl instance;

    protected ILogger logger;

    @Before
    public void before() {
        Config config = new Config();
        long time = System.currentTimeMillis();
        config.setInstanceName(this.getClass().getSimpleName() + "_" + time);
        config.setClusterName(this.getClass().getSimpleName() + "_" + time);
        instance = ((HazelcastInstanceProxy) HazelcastInstanceFactory.newHazelcastInstance(config,
            Thread.currentThread().getName(), new SeaTunnelNodeContext(new SeaTunnelConfig()))).getOriginal();
        nodeEngine = instance.node.nodeEngine;
        server = nodeEngine.getService(SeaTunnelServer.SERVICE_NAME);
        logger = nodeEngine.getLogger(this.getClass());
    }

    @After
    public void after() {
        server.shutdown(true);
        instance.shutdown();
    }
}
