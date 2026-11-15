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

package org.apache.seatunnel.e2e.common.container.spark;

import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.container.TestContainerId;

import com.google.auto.service.AutoService;
import lombok.NoArgsConstructor;

import java.io.File;
import java.util.Arrays;
import java.util.List;

@NoArgsConstructor
@AutoService(TestContainer.class)
public class Spark35Container extends AbstractTestSparkContainer {

    @Override
    public TestContainerId identifier() {
        return TestContainerId.SPARK_3_5;
    }

    @Override
    protected String getStartModuleName() {
        return "seatunnel-spark-starter" + File.separator + "seatunnel-spark-3.5-starter";
    }

    @Override
    protected String getDockerImage() {
        return "apache/spark:3.5.8";
    }

    @Override
    protected List<String> getDockerCommand() {
        return Arrays.asList(
                "/opt/spark/bin/spark-class",
                "org.apache.spark.deploy.master.Master",
                "--host",
                "0.0.0.0");
    }

    @Override
    protected String getStartShellName() {
        return "start-seatunnel-spark-3.5-connector-v2.sh";
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
