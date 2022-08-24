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

package org.apache.seatunnel.engine.server.checkpoint;

import static java.util.Collections.addAll;

import org.apache.seatunnel.api.common.SeaTunnelContext;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSink;
import org.apache.seatunnel.connectors.seatunnel.fake.source.FakeSource;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.utils.IdGenerator;
import org.apache.seatunnel.engine.core.dag.actions.Action;
import org.apache.seatunnel.engine.core.dag.actions.SinkAction;
import org.apache.seatunnel.engine.core.dag.actions.SourceAction;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.dag.logical.LogicalEdge;
import org.apache.seatunnel.engine.core.dag.logical.LogicalVertex;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.server.SeaTunnelNodeContext;
import org.apache.seatunnel.engine.server.dag.physical.PlanUtils;

import com.hazelcast.config.Config;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.instance.impl.HazelcastInstanceProxy;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.spi.impl.NodeEngine;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;


public class CheckpointPlanTest {

    private NodeEngine nodeEngine;

    private HazelcastInstanceImpl instance;

    @Before
    public void before() {
        Config config = new Config();
        config.setInstanceName("test");
        config.setClusterName("test");
        instance = ((HazelcastInstanceProxy) HazelcastInstanceFactory.newHazelcastInstance(config,
            this.getClass().getSimpleName(), new SeaTunnelNodeContext(new SeaTunnelConfig()))).getOriginal();
        nodeEngine = instance.node.nodeEngine;
    }

    @After
    public void after() {
        instance.shutdown();
    }

    @Test
    public void testGenerateCheckpointPlan() {
        final IdGenerator idGenerator = new IdGenerator();
        final LogicalDag logicalDag = new LogicalDag();
        fillVirtualVertex(idGenerator, logicalDag, 2);
        fillVirtualVertex(idGenerator, logicalDag, 3);

        JobConfig config = new JobConfig();
        config.setName("test");
        config.setMode(JobMode.BATCH);

        JobImmutableInformation jobInfo = new JobImmutableInformation(1,
            nodeEngine.getSerializationService().toData(logicalDag), config, Collections.emptyList());
        Map<Integer, CheckpointPlan> checkpointPlans = PlanUtils.fromLogicalDAG(logicalDag, nodeEngine,
            jobInfo,
            System.currentTimeMillis(),
            Executors.newCachedThreadPool(),
            instance.getFlakeIdGenerator(Constant.SEATUNNEL_ID_GENERATOR_NAME)).f1();
        Assert.assertNotNull(checkpointPlans);
        Assert.assertEquals(2, checkpointPlans.size());
        // enum(1) + reader(2) + writer(2)
        Assert.assertEquals(5, checkpointPlans.get(1).getPipelineTaskIds().size());
        // enum
        Assert.assertEquals(1, checkpointPlans.get(1).getStartingVertices().size());
        // enum + reader
        Assert.assertEquals(2, checkpointPlans.get(1).getStatefulVertices().size());
        // enum(1) + reader(3) + writer(3)
        Assert.assertEquals(7, checkpointPlans.get(2).getPipelineTaskIds().size());
        // enum
        Assert.assertEquals(1, checkpointPlans.get(2).getStartingVertices().size());
        // enum + reader
        Assert.assertEquals(2, checkpointPlans.get(2).getStatefulVertices().size());
    }

    private static void fillVirtualVertex(IdGenerator idGenerator, LogicalDag logicalDag, int parallelism) {
        SeaTunnelContext.getContext().setJobMode(JobMode.BATCH);
        FakeSource fakeSource = new FakeSource();
        fakeSource.setSeaTunnelContext(SeaTunnelContext.getContext());

        Action fake = new SourceAction<>(idGenerator.getNextId(), "fake", fakeSource, Collections.emptyList());
        fake.setParallelism(parallelism);
        LogicalVertex fakeVertex = new LogicalVertex(fake.getId(), fake, parallelism);

        ConsoleSink consoleSink = new ConsoleSink();
        consoleSink.setSeaTunnelContext(SeaTunnelContext.getContext());
        Action console = new SinkAction<>(idGenerator.getNextId(), "console", consoleSink, Collections.emptyList());
        console.setParallelism(parallelism);
        LogicalVertex consoleVertex = new LogicalVertex(console.getId(), console, parallelism);

        LogicalEdge edge = new LogicalEdge(fakeVertex, consoleVertex);

        logicalDag.getEdges().add(edge);
        logicalDag.addLogicalVertex(fakeVertex);
        logicalDag.addLogicalVertex(consoleVertex);
    }
}
