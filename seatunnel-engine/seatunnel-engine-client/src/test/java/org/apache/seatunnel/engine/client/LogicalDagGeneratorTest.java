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

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.utils.IdGenerator;
import org.apache.seatunnel.engine.core.dag.actions.Action;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDagGenerator;
import org.apache.seatunnel.engine.core.parse.MultipleTableJobConfigParser;

import org.apache.commons.lang3.tuple.ImmutablePair;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.internal.json.JsonObject;

import java.net.URL;
import java.util.List;
import java.util.Set;

public class LogicalDagGeneratorTest {
    @Test
    public void testLogicalGenerator() {
        Common.setDeployMode(DeployMode.CLIENT);
        String filePath = TestUtils.getResource("/batch_fakesource_to_file_complex.conf");
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("fake_to_file");
        jobConfig.setJobContext(new JobContext());

        IdGenerator idGenerator = new IdGenerator();
        ImmutablePair<List<Action>, Set<URL>> immutablePair =
                new MultipleTableJobConfigParser(filePath, idGenerator, jobConfig).parse();

        LogicalDagGenerator logicalDagGenerator =
                new LogicalDagGenerator(immutablePair.getLeft(), jobConfig, idGenerator);
        LogicalDag logicalDag = logicalDagGenerator.generate();
        JsonObject logicalDagJson = logicalDag.getLogicalDagAsJson();
        String result =
                "{\"vertices\":[{\"id\":1,\"name\":\"Source[0]-FakeSource-fake(id=1)\",\"parallelism\":3},{\"id\":2,\"name\":\"Source[0]-FakeSource-fake2(id=2)\",\"parallelism\":3},{\"id\":3,\"name\":\"Sink[0]-LocalFile-default-identifier(id=3)\",\"parallelism\":3}],\"edges\":[{\"inputVertex\":\"Source[0]-FakeSource-fake\",\"targetVertex\":\"Sink[0]-LocalFile-default-identifier\"},{\"inputVertex\":\"Source[0]-FakeSource-fake2\",\"targetVertex\":\"Sink[0]-LocalFile-default-identifier\"}]}";
        Assertions.assertEquals(result, logicalDagJson.toString());
    }
}
