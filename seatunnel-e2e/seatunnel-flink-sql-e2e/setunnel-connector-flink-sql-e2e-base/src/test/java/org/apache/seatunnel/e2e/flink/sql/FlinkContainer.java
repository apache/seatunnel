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

package org.apache.seatunnel.e2e.flink.sql;

import static org.apache.seatunnel.e2e.ContainerUtil.copyConfigFileToContainer;

import org.apache.seatunnel.e2e.flink.AbstractFlinkContainer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;

import java.io.IOException;

/**
 * This class is the base class of FlinkEnvironment test.
 * The before method will create a Flink cluster, and after method will close the Flink cluster.
 * You can use {@link FlinkContainer#executeSeaTunnelFlinkJob(String)} to submit a seatunnel config and run a seatunnel job.
 */
public abstract class FlinkContainer extends AbstractFlinkContainer {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkContainer.class);

    private static final String FLINK_DOCKER_IMAGE = "flink:1.13.6-scala_2.11";

    private static final String START_SHELL_NAME = "start-seatunnel-sql.sh";

    private static final String START_MODULE_NAME = "seatunnel-core-flink-sql";

    private static final String CONNECTORS_ROOT_PATH = "seatunnel-connectors/seatunnel-connectors-flink-sql";

    private static final String CONNECTOR_TYPE = "seatunnel-sql";

    private static final String CONNECTOR_PREFIX = "flink-sql-connector-";

    public FlinkContainer() {
        super(FLINK_DOCKER_IMAGE,
            START_SHELL_NAME,
            START_MODULE_NAME,
            CONNECTORS_ROOT_PATH,
            CONNECTOR_TYPE,
            CONNECTOR_PREFIX);
    }

    @Override
    public Container.ExecResult executeSeaTunnelFlinkJob(String confFile) throws IOException, InterruptedException {
        String confInContainerPath = copyConfigFileToContainer(jobManager, confFile);
        return executeCommand(confInContainerPath);
    }
}
