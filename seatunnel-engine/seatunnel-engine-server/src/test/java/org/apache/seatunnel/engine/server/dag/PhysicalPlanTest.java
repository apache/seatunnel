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

package org.apache.seatunnel.engine.server.dag;

import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.utils.IdGenerator;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.server.dag.execution.ExecutionPlan;
import org.apache.seatunnel.engine.server.dag.execution.ExecutionPlanGenerator;
import org.apache.seatunnel.engine.server.dag.physical.PhysicalPlan;
import org.apache.seatunnel.engine.server.dag.physical.PhysicalPlanGenerator;

import org.junit.Test;

public class PhysicalPlanTest {

    @Test
    public void testLogicalToPhysical() {

        JobConfig jobConfig = new JobConfig();

        LogicalDag logicalPlan = new LogicalDag(jobConfig, new IdGenerator());

        ExecutionPlan executionPlan = new ExecutionPlanGenerator(logicalPlan).generate();

        PhysicalPlan physicalPlan = new PhysicalPlanGenerator(executionPlan).generate();

        System.out.println(physicalPlan);
    }

}
