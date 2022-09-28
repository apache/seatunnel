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

import org.apache.seatunnel.e2e.common.AbstractSparkContainer;

/**
 * This class is the base class of SparkEnvironment test. The before method will create a Spark master, and after method will close the Spark master.
 * You can use {@link SparkContainer#executeSeaTunnelSparkJob} to submit a seatunnel conf and a seatunnel spark job.
 */
public abstract class SparkContainer extends AbstractSparkContainer {

    @Override
    public String identifier() {
        return "connector-v1/spark:2.4.3";
    }

    @Override
    protected String getStartModuleName() {
        return "seatunnel-core-spark";
    }

    @Override
    protected String getStartShellName() {
        return "start-seatunnel-spark.sh";
    }

    @Override
    protected String getConnectorType() {
        return "spark";
    }

    @Override
    protected String getConnectorModulePath() {
        return "seatunnel-connectors/seatunnel-connectors-spark";
    }

    @Override
    protected String getConnectorNamePrefix() {
        return "seatunnel-connector-spark-";
    }
}
