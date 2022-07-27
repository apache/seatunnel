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

package org.apache.seatunnel.engine.client;

import org.apache.seatunnel.engine.server.SeaTunnelNodeContext;

import com.google.common.collect.Lists;
import com.hazelcast.config.Config;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@SuppressWarnings("checkstyle:MagicNumber")
@RunWith(JUnit4.class)
public class SeaTunnelClientTest {

    @BeforeClass
    public static void beforeClass() throws Exception {
        Config config = new Config();
        config.getSecurityConfig().setEnabled(false);
        config.getJetConfig().setEnabled(false);
        config.getNetworkConfig().setPort(50001);
        HazelcastInstanceFactory.newHazelcastInstance(config, Thread.currentThread().getName(), new SeaTunnelNodeContext());
    }

    @Test
    public void testSayHello() {
        SeaTunnelClientConfig seaTunnelClientConfig = new SeaTunnelClientConfig();
        seaTunnelClientConfig.setClusterName("dev");
        seaTunnelClientConfig.getNetworkConfig().setAddresses(Lists.newArrayList("localhost:50001"));
        SeaTunnelClient engineClient = new SeaTunnelClient(seaTunnelClientConfig);

        String msg = "Hello world";
        String s = engineClient.printMessageToMaster(msg);
        Assert.assertEquals(msg, s);
    }
}
