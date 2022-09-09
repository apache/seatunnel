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

import org.apache.seatunnel.e2e.common.AbstractFlinkContainer;

/**
 * This class is the base class of FlinkEnvironment test.
 * The before method will create a Flink cluster, and after method will close the Flink cluster.
 * You can use {@link FlinkContainer#executeSeaTunnelFlinkJob(String)} to submit a seatunnel config and run a seatunnel job.
 */
public abstract class FlinkContainer extends AbstractFlinkContainer {

    @Override
    protected String getStartModulePath() {
        return "seatunnel-core-flink-sql";
    }

    @Override
    protected String getStartShellName() {
        return "start-seatunnel-sql.sh";
    }

    @Override
    protected String getConnectorType() {
        return "flink-sql";
    }

    @Override
    protected String getConnectorModulePath() {
        return "seatunnel-connectors/seatunnel-connectors-flink-sql";
    }

    @Override
    protected String getConnectorNamePrefix() {
        return "flink-sql-connector-";
    }
}
