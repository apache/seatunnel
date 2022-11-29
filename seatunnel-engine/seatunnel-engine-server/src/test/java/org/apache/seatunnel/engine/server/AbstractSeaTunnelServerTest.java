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

import org.apache.seatunnel.common.utils.ExceptionUtils;

import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngine;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;

@Slf4j
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractSeaTunnelServerTest<T extends AbstractSeaTunnelServerTest> {

    protected SeaTunnelServer server;

    protected NodeEngine nodeEngine;

    protected HazelcastInstanceImpl instance;

    protected static ILogger LOGGER;

    @BeforeAll
    public void before() {
        String name = ((T) this).getClass().getName();
        instance = SeaTunnelServerStarter.createHazelcastInstance(
            TestUtils.getClusterName("AbstractSeaTunnelServerTest_" + name));
        nodeEngine = instance.node.nodeEngine;
        server = nodeEngine.getService(SeaTunnelServer.SERVICE_NAME);
        LOGGER = nodeEngine.getLogger(AbstractSeaTunnelServerTest.class);
    }

    @AfterAll
    public void after() {
        try {
            if (server != null) {
                server.shutdown(true);
            }

            if (instance != null) {
                instance.shutdown();
            }
        } catch (Exception e) {
            log.error(ExceptionUtils.getMessage(e));
        }
    }
}
