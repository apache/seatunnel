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

package org.apache.seatunnel.e2e.spark;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is the base class of SparkEnvironment test. The before method will create a Spark master, and after method will close the Spark master.
 * You can use {@link SparkContainer#executeSeaTunnelSparkJob} to submit a seatunnel conf and a seatunnel spark job.
 */
public abstract class SparkContainer extends AbstractSparkContainer {

    private static final Logger LOG = LoggerFactory.getLogger(SparkContainer.class);

    private static final String START_SHELL_NAME = "start-seatunnel-spark-new-connector.sh";

    private static final String START_MODULE_NAME = "seatunnel-spark-starter";

    private static final String CONNECTORS_ROOT_PATH = "seatunnel-connectors-v2";

    private static final String CONNECTOR_TYPE = "seatunnel";

    private static final String CONNECTOR_PREFIX = "connector-";
    public SparkContainer() {
        super(START_SHELL_NAME,
            START_MODULE_NAME,
            CONNECTORS_ROOT_PATH,
            CONNECTOR_TYPE,
            CONNECTOR_PREFIX);
    }
}
