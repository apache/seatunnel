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

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSink;
import org.apache.seatunnel.connectors.seatunnel.fake.source.FakeSource;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.utils.IdGenerator;
import org.apache.seatunnel.engine.core.dag.actions.Action;
import org.apache.seatunnel.engine.core.dag.actions.SinkAction;
import org.apache.seatunnel.engine.core.dag.actions.SourceAction;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDagGenerator;
import org.apache.seatunnel.engine.core.dag.logical.LogicalEdge;
import org.apache.seatunnel.engine.core.dag.logical.LogicalVertex;
import org.apache.seatunnel.engine.core.parse.MultipleTableJobConfigParser;

import org.apache.commons.lang3.tuple.ImmutablePair;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

public class TestUtils {
    public static String getResource(String confFile) {
        return System.getProperty("user.dir") + "/src/test/resources/" + confFile;
    }

    public static LogicalDag getTestLogicalDag(JobContext jobContext) throws MalformedURLException {
        IdGenerator idGenerator = new IdGenerator();
        Config fakeSourceConfig =
                ConfigFactory.parseMap(
                        Collections.singletonMap(
                                "schema",
                                Collections.singletonMap(
                                        "fields", ImmutableMap.of("id", "int", "name", "string"))));
        FakeSource fakeSource = new FakeSource(ReadonlyConfig.fromConfig(fakeSourceConfig));
        fakeSource.setJobContext(jobContext);

        Action fake =
                new SourceAction<>(
                        idGenerator.getNextId(),
                        "fake",
                        fakeSource,
                        Sets.newHashSet(new URL("file:///fake.jar")),
                        Collections.emptySet());
        fake.setParallelism(3);
        LogicalVertex fakeVertex = new LogicalVertex(fake.getId(), fake, 3);

        ConsoleSink consoleSink =
                new ConsoleSink(
                        new SeaTunnelRowType(
                                new String[] {"id"},
                                new SeaTunnelDataType<?>[] {BasicType.INT_TYPE}),
                        ReadonlyConfig.fromMap(new HashMap<>()));
        consoleSink.setJobContext(jobContext);
        Action console =
                new SinkAction<>(
                        idGenerator.getNextId(),
                        "console",
                        consoleSink,
                        Sets.newHashSet(new URL("file:///console.jar")),
                        Collections.emptySet());
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

    public static LogicalDag createTestLogicalPlan(
            String jobConfigFile, String jobName, Long jobId) {
        Common.setDeployMode(DeployMode.CLIENT);
        JobContext jobContext = new JobContext(jobId);
        String filePath = TestUtils.getResource(jobConfigFile);
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName(jobName);
        jobConfig.setJobContext(jobContext);

        IdGenerator idGenerator = new IdGenerator();
        ImmutablePair<List<Action>, Set<URL>> immutablePair =
                new MultipleTableJobConfigParser(filePath, idGenerator, jobConfig).parse();

        LogicalDagGenerator logicalDagGenerator =
                new LogicalDagGenerator(immutablePair.getLeft(), jobConfig, idGenerator);
        return logicalDagGenerator.generate();
    }
}
