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

import org.apache.seatunnel.api.common.SeaTunnelContext;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSink;
import org.apache.seatunnel.connectors.seatunnel.fake.source.FakeSource;
import org.apache.seatunnel.engine.common.Constant;
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
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.dag.physical.PhysicalPlan;
import org.apache.seatunnel.engine.server.dag.physical.PhysicalPlanUtils;
import org.apache.seatunnel.engine.server.execution.TaskGroup;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.instance.impl.HazelcastInstanceProxy;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class TaskTest {

    private SeaTunnelServer service;

    @Before
    public void before() {
        HazelcastInstanceImpl instance = ((HazelcastInstanceProxy) HazelcastInstanceFactory.newHazelcastInstance(new Config(), Thread.currentThread().getName(), new SeaTunnelNodeContext(new SeaTunnelConfig()))).getOriginal();
        service = instance.node.nodeEngine.getService(SeaTunnelServer.SERVICE_NAME);
    }

    @Test
    public void testTask() throws MalformedURLException {

        IdGenerator idGenerator = new IdGenerator();

        SeaTunnelContext.getContext().setJobMode(JobMode.BATCH);
        FakeSource fakeSource = new FakeSource();
        fakeSource.setSeaTunnelContext(SeaTunnelContext.getContext());

        Action fake = new SourceAction<>(idGenerator.getNextId(), "fake", fakeSource,
                Collections.singletonList(new URL("file:///fake.jar")));
        LogicalVertex fakeVertex = new LogicalVertex(fake.getId(), fake, 2);

        FakeSource fakeSource2 = new FakeSource();
        fakeSource2.setSeaTunnelContext(SeaTunnelContext.getContext());
        Action fake2 = new SourceAction<>(idGenerator.getNextId(), "fake", fakeSource2,
                Collections.singletonList(new URL("file:///fake.jar")));
        LogicalVertex fake2Vertex = new LogicalVertex(fake2.getId(), fake2, 2);

        ConsoleSink consoleSink = new ConsoleSink();
        consoleSink.setSeaTunnelContext(SeaTunnelContext.getContext());
        Action console = new SinkAction<>(idGenerator.getNextId(), "console", consoleSink,
                Collections.singletonList(new URL("file:///console.jar")));
        LogicalVertex consoleVertex = new LogicalVertex(console.getId(), console, 2);

        LogicalEdge edge = new LogicalEdge(fakeVertex, consoleVertex);

        LogicalDag logicalDag = new LogicalDag();
        logicalDag.addLogicalVertex(fakeVertex);
        logicalDag.addLogicalVertex(consoleVertex);
        logicalDag.addLogicalVertex(fake2Vertex);
        logicalDag.addEdge(edge);

        JobImmutableInformation jobImmutableInformation = new JobImmutableInformation();

        Config config = new Config();
        config.setClusterName("test");
        config.setInstanceName("local");

        PhysicalPlan physicalPlan = PhysicalPlanUtils.fromLogicalDAG(logicalDag, jobImmutableInformation,
                System.currentTimeMillis(),
                Executors.newCachedThreadPool(),
                Hazelcast.getOrCreateHazelcastInstance(config).getFlakeIdGenerator(Constant.SEATUNNEL_ID_GENERATOR_NAME));

        List<CompletableFuture<?>> future = new ArrayList<>();
        physicalPlan.getPipelineList().forEach(subPlan -> {
            future.addAll(subPlan.getCoordinatorVertexList().stream().map(vertx -> {
                try {
                    Field field = vertx.getClass().getDeclaredField("taskGroup");
                    field.setAccessible(true);
                    TaskGroup group = (TaskGroup) field.get(vertx);
                    return service.getTaskExecutionService().submitTaskGroup(group, new CompletableFuture<>());
                } catch (NoSuchFieldException | IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }).collect(Collectors.toList()));

            future.addAll(subPlan.getPhysicalVertexList().stream().map(vertx -> {
                try {
                    Field field = vertx.getClass().getDeclaredField("taskGroup");
                    field.setAccessible(true);
                    TaskGroup group = (TaskGroup) field.get(vertx);
                    return service.getTaskExecutionService().submitTaskGroup(group, new CompletableFuture<>());
                } catch (NoSuchFieldException | IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }).collect(Collectors.toList()));
        });
        for (CompletableFuture<?> completableFuture : future) {
            completableFuture.join();
        }
    }

}
