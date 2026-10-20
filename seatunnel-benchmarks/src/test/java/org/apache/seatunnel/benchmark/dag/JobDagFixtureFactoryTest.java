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

package org.apache.seatunnel.benchmark.dag;

import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.engine.core.job.Edge;
import org.apache.seatunnel.engine.core.job.JobDAGInfo;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JobDagFixtureFactoryTest {

    @Test
    void createsSourceToSinkPipelines() {
        JobDAGInfo dag = JobDagFixtureFactory.create(10);

        assertEquals(10, dag.getPipelineEdges().size());
        assertEquals(20, dag.getVertexInfoMap().size());
        dag.getPipelineEdges()
                .values()
                .forEach(
                        edges -> {
                            assertEquals(1, edges.size());
                            Edge edge = edges.get(0);
                            assertEquals(
                                    PluginType.SOURCE,
                                    dag.getVertexInfoMap().get(edge.getInputVertexId()).getType());
                            assertEquals(
                                    PluginType.SINK,
                                    dag.getVertexInfoMap().get(edge.getTargetVertexId()).getType());
                        });
    }

    @Test
    void rejectsEmptyDag() {
        assertThrows(IllegalArgumentException.class, () -> JobDagFixtureFactory.create(0));
    }
}
