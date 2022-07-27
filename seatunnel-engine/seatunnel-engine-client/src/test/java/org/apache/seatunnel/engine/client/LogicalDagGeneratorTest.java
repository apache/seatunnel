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

import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.core.dag.actions.Action;
import org.apache.seatunnel.engine.core.dag.logicaldag.LogicalDag;
import org.apache.seatunnel.engine.core.dag.logicaldag.LogicalDagGenerator;

import com.google.common.collect.Lists;
import com.hazelcast.internal.json.JsonObject;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.List;

@RunWith(JUnit4.class)
public class LogicalDagGeneratorTest {
    @Test
    public void testLogicalGenerator() {
        Common.setDeployMode(DeployMode.CLIENT);
        String filePath = this.getClass().getResource("/fakesource_to_file_complex.conf").getFile();
        JobConfigParse jobConfigParse = new JobConfigParse(filePath);
        List<Action> actions = jobConfigParse.parse();

        SeaTunnelClientConfig seaTunnelClientConfig = new SeaTunnelClientConfig();
        seaTunnelClientConfig.setClusterName("dev");
        seaTunnelClientConfig.getNetworkConfig().setAddresses(Lists.newArrayList("localhost:50001"));
        JobConfig jobConfig = new JobConfig();
        jobConfig.setBoundedness(Boundedness.BOUNDED);
        jobConfig.setName("fake_to_file");
        JobExecutionEnvironment jobExecutionEnv = new JobExecutionEnvironment(jobConfig);
        jobExecutionEnv.addAction(actions);

        LogicalDagGenerator logicalDagGenerator = jobExecutionEnv.getLogicalDagGenerator();
        LogicalDag logicalDag = logicalDagGenerator.generate();
        JsonObject logicalDagJson = logicalDag.getLogicalDagAsJson();
        String result =
            "{\"vertices\":[{\"id\":1,\"name\":\"FakeSource(id=1)\",\"parallelism\":3},{\"id\":2,\"name\":\"FakeSource(id=2)\",\"parallelism\":3},{\"id\":0,\"name\":\"LocalFile(id=0)\",\"parallelism\":6}],\"edges\":[{\"leftVertex\":\"FakeSource(id=1)\",\"rightVertex\":\"LocalFile(id=0)\"},{\"leftVertex\":\"FakeSource(id=2)\",\"rightVertex\":\"LocalFile(id=0)\"}]}";
        Assert.assertEquals(result, logicalDagJson.toString());
    }
}
