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

package org.apache.seatunnel.engine.server.dag;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSink;
import org.apache.seatunnel.connectors.seatunnel.fake.source.FakeSource;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.config.EngineConfig;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.server.QueueType;
import org.apache.seatunnel.engine.common.utils.IdGenerator;
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.core.dag.actions.Action;
import org.apache.seatunnel.engine.core.dag.actions.SinkAction;
import org.apache.seatunnel.engine.core.dag.actions.SourceAction;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.dag.logical.LogicalEdge;
import org.apache.seatunnel.engine.core.dag.logical.LogicalVertex;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.server.AbstractSeaTunnelServerTest;
import org.apache.seatunnel.engine.server.TestUtils;
import org.apache.seatunnel.engine.server.dag.physical.PhysicalPlan;
import org.apache.seatunnel.engine.server.dag.physical.PlanUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.hazelcast.map.IMap;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.Executors;

public class TaskTest extends AbstractSeaTunnelServerTest {

    @Test
    public void testTask() throws MalformedURLException {
        JobContext jobContext = new JobContext();
        jobContext.setJobMode(JobMode.BATCH);
        LogicalDag testLogicalDag = TestUtils.getTestLogicalDag(jobContext);

        JobConfig config = new JobConfig();
        config.setName("test");

        JobImmutableInformation jobImmutableInformation =
                new JobImmutableInformation(
                        1,
                        "Test",
                        nodeEngine.getSerializationService().toData(testLogicalDag),
                        config,
                        Collections.emptyList(),
                        Collections.emptyList());

        PassiveCompletableFuture<Void> voidPassiveCompletableFuture =
                server.getCoordinatorService()
                        .submitJob(
                                jobImmutableInformation.getJobId(),
                                nodeEngine
                                        .getSerializationService()
                                        .toData(jobImmutableInformation),
                                jobImmutableInformation.isStartWithSavePoint());

        Assertions.assertNotNull(voidPassiveCompletableFuture);
    }

    @Test
    public void testLogicalToPhysical() throws MalformedURLException {

        IdGenerator idGenerator = new IdGenerator();

        Action fake =
                new SourceAction<>(
                        idGenerator.getNextId(),
                        "fake",
                        createFakeSource(),
                        Sets.newHashSet(new URL("file:///fake.jar")),
                        Collections.emptySet());
        LogicalVertex fakeVertex = new LogicalVertex(fake.getId(), fake, 2);

        Action fake2 =
                new SourceAction<>(
                        idGenerator.getNextId(),
                        "fake",
                        createFakeSource(),
                        Sets.newHashSet(new URL("file:///fake.jar")),
                        Collections.emptySet());
        LogicalVertex fake2Vertex = new LogicalVertex(fake2.getId(), fake2, 2);

        Action console =
                new SinkAction<>(
                        idGenerator.getNextId(),
                        "console",
                        new ConsoleSink(
                                new SeaTunnelRowType(
                                        new String[] {"id"},
                                        new SeaTunnelDataType<?>[] {BasicType.INT_TYPE}),
                                ReadonlyConfig.fromMap(new HashMap<>())),
                        Sets.newHashSet(new URL("file:///console.jar")),
                        Collections.emptySet());
        LogicalVertex consoleVertex = new LogicalVertex(console.getId(), console, 2);

        LogicalEdge edge = new LogicalEdge(fakeVertex, consoleVertex);

        LogicalDag logicalDag = new LogicalDag();
        logicalDag.addLogicalVertex(fakeVertex);
        logicalDag.addLogicalVertex(consoleVertex);
        logicalDag.addEdge(edge);

        JobConfig config = new JobConfig();
        config.setName("test");

        JobImmutableInformation jobImmutableInformation =
                new JobImmutableInformation(
                        1,
                        "Test",
                        nodeEngine.getSerializationService().toData(logicalDag),
                        config,
                        Collections.emptyList(),
                        Collections.emptyList());

        IMap<Object, Object> runningJobState =
                nodeEngine.getHazelcastInstance().getMap("testRunningJobState");
        IMap<Object, Long[]> runningJobStateTimestamp =
                nodeEngine.getHazelcastInstance().getMap("testRunningJobStateTimestamp");

        PhysicalPlan physicalPlan =
                PlanUtils.fromLogicalDAG(
                                logicalDag,
                                nodeEngine,
                                jobImmutableInformation,
                                System.currentTimeMillis(),
                                Executors.newCachedThreadPool(),
                                instance.getFlakeIdGenerator(Constant.SEATUNNEL_ID_GENERATOR_NAME),
                                runningJobState,
                                runningJobStateTimestamp,
                                QueueType.BLOCKINGQUEUE,
                                new EngineConfig())
                        .f0();

        Assertions.assertEquals(physicalPlan.getPipelineList().size(), 1);
        Assertions.assertEquals(
                physicalPlan.getPipelineList().get(0).getCoordinatorVertexList().size(), 1);
        Assertions.assertEquals(
                physicalPlan.getPipelineList().get(0).getPhysicalVertexList().size(), 2);
        Assertions.assertEquals(
                physicalPlan
                        .getPipelineList()
                        .get(0)
                        .getPhysicalVertexList()
                        .get(0)
                        .getTaskGroupImmutableInformation()
                        .getTasksData()
                        .size(),
                2);
        Assertions.assertEquals(
                physicalPlan
                        .getPipelineList()
                        .get(0)
                        .getPhysicalVertexList()
                        .get(0)
                        .getTaskGroupImmutableInformation()
                        .getJars()
                        .get(0),
                Sets.newHashSet(new URL("file:///fake.jar")));
        Assertions.assertEquals(
                physicalPlan
                        .getPipelineList()
                        .get(0)
                        .getPhysicalVertexList()
                        .get(0)
                        .getTaskGroupImmutableInformation()
                        .getJars()
                        .get(1),
                Sets.newHashSet(new URL("file:///console.jar")));
    }

    private static FakeSource createFakeSource() {
        Config fakeSourceConfig =
                ConfigFactory.parseMap(
                        Collections.singletonMap(
                                "schema",
                                Collections.singletonMap(
                                        "fields", ImmutableMap.of("id", "int", "name", "string"))));
        return new FakeSource(ReadonlyConfig.fromConfig(fakeSourceConfig));
    }
}
