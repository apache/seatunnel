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

package org.apache.seatunnel.e2e.common.container.flink;

import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.container.TestContainerId;

import com.google.auto.service.AutoService;
import lombok.NoArgsConstructor;

import java.io.File;

/**
 * This class is the base class of FlinkEnvironment test for new seatunnel connector API. The before
 * method will create a Flink cluster, and after method will close the Flink cluster. You can use
 * {@link Flink17Container#executeJob} to submit a seatunnel config and run a seatunnel job.
 */
@NoArgsConstructor
@AutoService(TestContainer.class)
public class Flink17Container extends AbstractTestFlinkContainer {

    @Override
    public TestContainerId identifier() {
        return TestContainerId.FLINK_1_17;
    }

    @Override
    protected String getDockerImage() {
        return "tyrantlucifer/flink:1.17.2-scala_2.12_hadoop27";
    }

    @Override
    protected String getStartModuleName() {
        return "seatunnel-flink-starter" + File.separator + "seatunnel-flink-15-starter";
    }

    @Override
    protected String getStartShellName() {
        return "start-seatunnel-flink-15-connector-v2.sh";
    }

    @Override
    protected String getConnectorType() {
        return "seatunnel";
    }

    @Override
    protected String getConnectorModulePath() {
        return "seatunnel-connectors-v2";
    }

    @Override
    protected String getConnectorNamePrefix() {
        return "connector-";
    }
}
