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

package org.apache.seatunnel.e2e.flink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is the base class of FlinkEnvironment test for new seatunnel connector API.
 * The before method will create a Flink cluster, and after method will close the Flink cluster.
 * You can use {@link FlinkContainer#executeSeaTunnelFlinkJob} to submit a seatunnel config and run a seatunnel job.
 */
public abstract class FlinkContainer extends AbstractFlinkContainer {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkContainer.class);

    private static final String FLINK_DOCKER_IMAGE = "tyrantlucifer/flink:1.13.6-scala_2.11_hadoop27";

    private static final String START_SHELL_NAME = "start-seatunnel-flink-new-connector.sh";

    private static final String START_MODULE_NAME = "seatunnel-flink-starter";

    private static final String CONNECTORS_ROOT_PATH = "seatunnel-connectors-v2";

    private static final String CONNECTOR_TYPE = "seatunnel";

    private static final String CONNECTOR_PREFIX = "connector-";
    public FlinkContainer() {
        super(FLINK_DOCKER_IMAGE,
            START_SHELL_NAME,
            START_MODULE_NAME,
            CONNECTORS_ROOT_PATH,
            CONNECTOR_TYPE,
            CONNECTOR_PREFIX);
    }

}
