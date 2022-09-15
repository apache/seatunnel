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

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSink;
import org.apache.seatunnel.connectors.seatunnel.fake.source.FakeSource;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.utils.IdGenerator;
import org.apache.seatunnel.engine.core.dag.actions.Action;
import org.apache.seatunnel.engine.core.dag.actions.SinkAction;
import org.apache.seatunnel.engine.core.dag.actions.SourceAction;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.dag.logical.LogicalEdge;
import org.apache.seatunnel.engine.core.dag.logical.LogicalVertex;

import com.google.common.collect.Sets;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.instance.impl.HazelcastInstanceProxy;

import java.net.MalformedURLException;
import java.net.URL;

public class TestUtils {

    @SuppressWarnings("checkstyle:MagicNumber")
    public static LogicalDag getTestLogicalDag(JobContext jobContext) throws MalformedURLException {
        IdGenerator idGenerator = new IdGenerator();
        FakeSource fakeSource = new FakeSource();
        fakeSource.setJobContext(jobContext);

        Action fake = new SourceAction<>(idGenerator.getNextId(), "fake", fakeSource,
            Sets.newHashSet(new URL("file:///fake.jar")));
        fake.setParallelism(3);
        LogicalVertex fakeVertex = new LogicalVertex(fake.getId(), fake, 3);

        ConsoleSink consoleSink = new ConsoleSink();
        consoleSink.setJobContext(jobContext);
        Action console = new SinkAction<>(idGenerator.getNextId(), "console", consoleSink,
            Sets.newHashSet(new URL("file:///console.jar")));
        console.setParallelism(3);
        LogicalVertex consoleVertex = new LogicalVertex(console.getId(), console, 3);

        LogicalEdge edge = new LogicalEdge(fakeVertex, consoleVertex);

        LogicalDag logicalDag = new LogicalDag();
        logicalDag.addLogicalVertex(fakeVertex);
        logicalDag.addLogicalVertex(consoleVertex);
        logicalDag.addEdge(edge);
        return logicalDag;
    }

    public static String getClusterName(String testClassName) {
        return System.getProperty("user.name") + "_" + testClassName;
    }

    public static HazelcastInstanceImpl createHazelcastInstance(String clusterNamePrefix) {
        SeaTunnelConfig seaTunnelConfig = new SeaTunnelConfig();
        seaTunnelConfig.getHazelcastConfig()
            .setClusterName(TestUtils.getClusterName(clusterNamePrefix + "_" + System.currentTimeMillis()));
        return ((HazelcastInstanceProxy) HazelcastInstanceFactory.newHazelcastInstance(
            seaTunnelConfig.getHazelcastConfig(),
            HazelcastInstanceFactory.createInstanceName(seaTunnelConfig.getHazelcastConfig()),
            new SeaTunnelNodeContext(seaTunnelConfig))).getOriginal();
    }
}
