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
import org.apache.seatunnel.e2e.common.util.ContainerUtil;

import org.testcontainers.containers.GenericContainer;

import com.google.auto.service.AutoService;
import lombok.NoArgsConstructor;

import java.io.File;

/**
 * This class is the base class of Spark 4.1 Environment test. The before method will create a Spark
 * master, and after method will close the Spark master. You can use {@link
 * Spark4Container#executeJob} to submit a seatunnel conf and a seatunnel spark job.
 */
@NoArgsConstructor
@AutoService(TestContainer.class)
public class Spark4Container extends AbstractTestSparkContainer {

    @Override
    public TestContainerId identifier() {
        return TestContainerId.SPARK_4_1;
    }

    @Override
    protected String getStartModuleName() {
        return "seatunnel-spark-starter" + File.separator + "seatunnel-spark-4.1-starter";
    }

    @Override
    protected String getDockerImage() {
        return "apache/spark:4.1.2";
    }

    @Override
    protected String getStartShellName() {
        return "start-seatunnel-spark-4.1-connector-v2.sh";
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

    /**
     * The official apache/spark image does not support Bitnami's SPARK_MODE env var. Start the
     * master process explicitly instead.
     */
    @Override
    protected void configureSparkMasterContainer(GenericContainer<?> container) {
        container.withCommand(
                "/opt/spark/bin/spark-class",
                "org.apache.spark.deploy.master.Master",
                "--host",
                "0.0.0.0");
    }

    /**
     * Spark 4.1 runs on Scala 2.13 while seatunnel-transforms-v2 still bundles Scala 2.12. Skip
     * transform jars until transforms-v2 is Spark 4.1 compatible.
     */
    @Override
    protected void copySeaTunnelStarterToContainer(GenericContainer<?> container) {
        ContainerUtil.copySeaTunnelStarterToContainer(
                container, this.startModuleName, this.startModuleFullPath, SEATUNNEL_HOME, false);
        ContainerUtil.copySpark41ScalaLibrariesToContainer(
                container, this.startModuleFullPath, SEATUNNEL_HOME);
    }
}
