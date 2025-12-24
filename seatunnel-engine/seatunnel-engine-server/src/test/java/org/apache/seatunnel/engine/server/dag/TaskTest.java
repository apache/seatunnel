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

import org.apache.seatunnel.shade.com.google.common.collect.ImmutableMap;
import org.apache.seatunnel.shade.com.google.common.collect.Sets;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
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
import org.apache.seatunnel.engine.server.dag.physical.PhysicalVertex;
import org.apache.seatunnel.engine.server.dag.physical.PlanUtils;
import org.apache.seatunnel.engine.server.dag.physical.SubPlan;
import org.apache.seatunnel.engine.server.execution.Task;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetEnvironmentVariable;

import com.hazelcast.map.IMap;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Executors;

import static org.apache.seatunnel.engine.core.classloader.DefaultClassLoaderService.SKIP_CHECK_JAR;

public class TaskTest extends AbstractSeaTunnelServerTest {

    @Test
    public void testTask() throws MalformedURLException {
        Long jobId = 1L;
        JobContext jobContext = new JobContext(jobId);
        jobContext.setJobMode(JobMode.BATCH);
        JobConfig config = new JobConfig();
        config.setName("test");
        config.setJobContext(jobContext);
        LogicalDag testLogicalDag = TestUtils.getTestLogicalDag(jobContext, config);

        JobImmutableInformation jobImmutableInformation =
                new JobImmutableInformation(
                        jobId,
                        "Test",
                        nodeEngine.getSerializationService(),
                        testLogicalDag,
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
    @SetEnvironmentVariable(key = SKIP_CHECK_JAR, value = "true")
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

        List<Column> columns = new ArrayList<>();
        columns.add(PhysicalColumn.of("id", BasicType.INT_TYPE, 11L, 0, true, 111, ""));

        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("default", TablePath.DEFAULT),
                        TableSchema.builder().columns(columns).build(),
                        new HashMap<>(),
                        Collections.emptyList(),
                        "fake");

        Action console =
                new SinkAction<>(
                        idGenerator.getNextId(),
                        "console",
                        new ConsoleSink(catalogTable, ReadonlyConfig.fromMap(new HashMap<>())),
                        Sets.newHashSet(new URL("file:///console.jar")),
                        Collections.emptySet());
        LogicalVertex consoleVertex = new LogicalVertex(console.getId(), console, 2);

        LogicalEdge edge = new LogicalEdge(fakeVertex, consoleVertex);

        JobConfig config = new JobConfig();
        config.setName("test");
        LogicalDag logicalDag = new LogicalDag(config, idGenerator);
        logicalDag.addLogicalVertex(fakeVertex);
        logicalDag.addLogicalVertex(consoleVertex);
        logicalDag.addEdge(edge);

        JobImmutableInformation jobImmutableInformation =
                new JobImmutableInformation(
                        1,
                        "Test",
                        nodeEngine.getSerializationService(),
                        logicalDag,
                        Collections.emptyList(),
                        Collections.emptyList());

        Assertions.assertEquals(2, jobImmutableInformation.getLogicalVertexJarsList().size());
        Assertions.assertIterableEquals(
                Sets.newHashSet(new URL("file:///fake.jar")),
                jobImmutableInformation.getLogicalVertexJarsList().get(0));
        Assertions.assertIterableEquals(
                Sets.newHashSet(new URL("file:///console.jar")),
                jobImmutableInformation.getLogicalVertexJarsList().get(1));

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
                                server.getClassLoaderService(),
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

    @Test
    public void testTaskGroupAndTaskLocationInfos() {
        Long jobId = 1L;
        LogicalDag testLogicalDag =
                TestUtils.createTestLogicalPlan(
                        "stream_fake_to_console.conf", "test_task_group_info", jobId);
        JobImmutableInformation jobImmutableInformation =
                new JobImmutableInformation(
                        jobId,
                        "Test",
                        nodeEngine.getSerializationService(),
                        testLogicalDag,
                        Collections.emptyList(),
                        Collections.emptyList());
        IMap<Object, Object> runningJobState =
                nodeEngine.getHazelcastInstance().getMap("testRunningJobState");
        IMap<Object, Long[]> runningJobStateTimestamp =
                nodeEngine.getHazelcastInstance().getMap("testRunningJobStateTimestamp");
        PhysicalPlan physicalPlan =
                PlanUtils.fromLogicalDAG(
                                testLogicalDag,
                                nodeEngine,
                                jobImmutableInformation,
                                System.currentTimeMillis(),
                                Executors.newCachedThreadPool(),
                                server.getClassLoaderService(),
                                instance.getFlakeIdGenerator(Constant.SEATUNNEL_ID_GENERATOR_NAME),
                                runningJobState,
                                runningJobStateTimestamp,
                                QueueType.BLOCKINGQUEUE,
                                new EngineConfig())
                        .f0();
        Assertions.assertEquals(2, physicalPlan.getPipelineList().size());
        for (int i = 0; i < physicalPlan.getPipelineList().size(); i++) {
            SubPlan subPlan = physicalPlan.getPipelineList().get(i);
            int pipelineId = subPlan.getPipelineId();

            for (int j = 0; j < subPlan.getCoordinatorVertexList().size(); j++) {
                PhysicalVertex physicalVertex = subPlan.getCoordinatorVertexList().get(j);
                TaskGroupLocation taskGroupLocation = physicalVertex.getTaskGroupLocation();
                List<Task> physicalTasks =
                        new ArrayList<>(physicalVertex.getTaskGroup().getTasks());
                for (int taskInGroupIndex = 0;
                        taskInGroupIndex < physicalTasks.size();
                        taskInGroupIndex++) {
                    Task task = physicalTasks.get(taskInGroupIndex);
                    long expectedTaskId =
                            pipelineId * 10000L * 10000L * 10000L
                                    + taskGroupLocation.getTaskGroupId() * 10000L * 10000L
                                    + taskInGroupIndex * 10000L;
                    Assertions.assertEquals(expectedTaskId, task.getTaskID());
                }
            }

            for (int j = 0; j < subPlan.getPhysicalVertexList().size(); j++) {
                PhysicalVertex physicalVertex = subPlan.getPhysicalVertexList().get(j);
                TaskGroupLocation taskGroupLocation = physicalVertex.getTaskGroupLocation();
                List<Task> physicalTasks =
                        new ArrayList<>(physicalVertex.getTaskGroup().getTasks());
                for (int taskInGroupIndex = 0;
                        taskInGroupIndex < physicalTasks.size();
                        taskInGroupIndex++) {
                    Task task = physicalTasks.get(taskInGroupIndex);
                    // can't get job parallel index, use prefix check
                    long expectedTaskIdPrefix =
                            pipelineId * 10000L * 10000L * 10000L
                                    + taskGroupLocation.getTaskGroupId() * 10000L * 10000L
                                    + taskInGroupIndex * 10000L;
                    Assertions.assertEquals(
                            expectedTaskIdPrefix / 10000L, task.getTaskID() / 10000L);
                }
            }
        }
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
