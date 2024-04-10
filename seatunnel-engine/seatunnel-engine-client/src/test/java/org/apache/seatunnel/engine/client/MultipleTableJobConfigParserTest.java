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

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.core.starter.utils.ConfigBuilder;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.utils.IdGenerator;
import org.apache.seatunnel.engine.core.dag.actions.Action;
import org.apache.seatunnel.engine.core.dag.actions.SinkAction;
import org.apache.seatunnel.engine.core.parse.MultipleTableJobConfigParser;

import org.apache.commons.lang3.tuple.ImmutablePair;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class MultipleTableJobConfigParserTest {

    @Test
    public void testSimpleJobParse() {
        Common.setDeployMode(DeployMode.CLIENT);
        String filePath = TestUtils.getResource("/batch_fakesource_to_file.conf");
        JobConfig jobConfig = new JobConfig();
        jobConfig.setJobContext(new JobContext());
        MultipleTableJobConfigParser jobConfigParser =
                new MultipleTableJobConfigParser(filePath, new IdGenerator(), jobConfig);
        ImmutablePair<List<Action>, Set<URL>> parse = jobConfigParser.parse(null);
        List<Action> actions = parse.getLeft();
        Assertions.assertEquals(1, actions.size());
        Assertions.assertEquals("Sink[0]-LocalFile-MultiTableSink", actions.get(0).getName());
        Assertions.assertEquals(1, actions.get(0).getUpstream().size());
        Assertions.assertEquals(
                "Source[0]-FakeSource", actions.get(0).getUpstream().get(0).getName());

        Assertions.assertEquals(3, actions.get(0).getUpstream().get(0).getParallelism());
        Assertions.assertEquals(3, actions.get(0).getParallelism());
    }

    @Test
    public void testComplexJobParse() {
        Common.setDeployMode(DeployMode.CLIENT);
        String filePath = TestUtils.getResource("/batch_fakesource_to_file_complex.conf");
        JobConfig jobConfig = new JobConfig();
        jobConfig.setJobContext(new JobContext());
        MultipleTableJobConfigParser jobConfigParser =
                new MultipleTableJobConfigParser(filePath, new IdGenerator(), jobConfig);
        ImmutablePair<List<Action>, Set<URL>> parse = jobConfigParser.parse(null);
        List<Action> actions = parse.getLeft();
        Assertions.assertEquals(1, actions.size());

        Assertions.assertEquals("Sink[0]-LocalFile-fake", actions.get(0).getName());
        Assertions.assertEquals(2, actions.get(0).getUpstream().size());

        String[] expected = {"Source[0]-FakeSource", "Source[1]-FakeSource"};
        String[] actual = {
            actions.get(0).getUpstream().get(0).getName(),
            actions.get(0).getUpstream().get(1).getName()
        };

        Arrays.sort(expected);
        Arrays.sort(actual);

        Assertions.assertArrayEquals(expected, actual);

        Assertions.assertEquals(3, actions.get(0).getUpstream().get(0).getParallelism());
        Assertions.assertEquals(3, actions.get(0).getUpstream().get(1).getParallelism());
        Assertions.assertEquals(3, actions.get(0).getParallelism());
    }

    @Test
    public void testMultipleSinkName() {
        Common.setDeployMode(DeployMode.CLIENT);
        String filePath = TestUtils.getResource("/batch_fakesource_to_two_file.conf");
        JobConfig jobConfig = new JobConfig();
        jobConfig.setJobContext(new JobContext());
        MultipleTableJobConfigParser jobConfigParser =
                new MultipleTableJobConfigParser(filePath, new IdGenerator(), jobConfig);
        ImmutablePair<List<Action>, Set<URL>> parse = jobConfigParser.parse(null);
        List<Action> actions = parse.getLeft();
        Assertions.assertEquals(2, actions.size());

        // This is union sink
        Assertions.assertEquals("Sink[0]-LocalFile-fake", actions.get(0).getName());

        // This is multiple table sink
        Assertions.assertEquals("Sink[1]-LocalFile-MultiTableSink", actions.get(1).getName());
    }

    @Test
    public void testMultipleTableSourceWithMultiTableSinkParse() throws IOException {
        Common.setDeployMode(DeployMode.CLIENT);
        String filePath = TestUtils.getResource("/batch_fake_to_console_multi_table.conf");
        JobConfig jobConfig = new JobConfig();
        jobConfig.setJobContext(new JobContext());
        Config config = ConfigBuilder.of(Paths.get(filePath));
        MultipleTableJobConfigParser jobConfigParser =
                new MultipleTableJobConfigParser(config, new IdGenerator(), jobConfig);
        ImmutablePair<List<Action>, Set<URL>> parse = jobConfigParser.parse(null);
        List<Action> actions = parse.getLeft();
        Assertions.assertEquals(1, actions.size());
        Assertions.assertEquals("Sink[0]-console-MultiTableSink", actions.get(0).getName());
        Assertions.assertFalse(
                ((SinkAction) actions.get(0)).getSink().createCommitter().isPresent());
        Assertions.assertFalse(
                ((SinkAction) actions.get(0)).getSink().createAggregatedCommitter().isPresent());
    }

    @Test
    public void testDuplicatedTransformInOnePipeline() {
        Common.setDeployMode(DeployMode.CLIENT);
        String filePath =
                TestUtils.getResource("/batch_fake_to_console_with_duplicated_transform.conf");
        JobConfig jobConfig = new JobConfig();
        jobConfig.setJobContext(new JobContext());
        Config config = ConfigBuilder.of(Paths.get(filePath));
        MultipleTableJobConfigParser jobConfigParser =
                new MultipleTableJobConfigParser(config, new IdGenerator(), jobConfig);
        ImmutablePair<List<Action>, Set<URL>> parse = jobConfigParser.parse(null);
        List<Action> actions = parse.getLeft();
        Assertions.assertEquals("Transform[0]-sql", actions.get(0).getUpstream().get(0).getName());
        Assertions.assertEquals("Transform[1]-sql", actions.get(1).getUpstream().get(0).getName());
    }
}
