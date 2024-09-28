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

package org.apache.seatunnel.engine.server.rest.servlet;

import org.apache.seatunnel.api.common.metrics.JobMetrics;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.core.job.JobDAGInfo;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.master.JobHistoryService.JobState;
import org.apache.seatunnel.engine.server.operation.GetJobMetricsOperation;
import org.apache.seatunnel.engine.server.utils.NodeEngineUtil;

import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.NodeEngineImpl;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.util.Comparator;
import java.util.Map;

import static org.apache.seatunnel.engine.server.rest.RestHttpGetCommandProcessor.getJobInfoJson;

public class FinishedJobsServlet extends BaseServlet {

    private static final long serialVersionUID = 1L;

    public FinishedJobsServlet(NodeEngineImpl nodeEngine) {
        super(nodeEngine);
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {

        Map<String, String> state = getParameterMap(req);

        IMap<Long, JobState> finishedJob =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_FINISHED_JOB_STATE);

        IMap<Long, JobMetrics> finishedJobMetrics =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_FINISHED_JOB_METRICS);

        IMap<Long, JobDAGInfo> finishedJobDAGInfo =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_FINISHED_JOB_VERTEX_INFO);
        SeaTunnelServer seaTunnelServer = getSeaTunnelServer(true);
        JsonArray jobs =
                finishedJob.values().stream()
                        .filter(
                                jobState -> {
                                    if (state.isEmpty()) {
                                        return true;
                                    }
                                    return jobState.getJobStatus()
                                            .name()
                                            .equals(state.get("state").toUpperCase());
                                })
                        .sorted(Comparator.comparing(JobState::getFinishTime))
                        .map(
                                jobState -> {
                                    Long jobId = jobState.getJobId();
                                    String jobMetrics;
                                    if (seaTunnelServer == null) {
                                        jobMetrics =
                                                (String)
                                                        NodeEngineUtil.sendOperationToMasterNode(
                                                                        nodeEngine,
                                                                        new GetJobMetricsOperation(
                                                                                jobId))
                                                                .join();
                                    } else {
                                        jobMetrics =
                                                seaTunnelServer
                                                        .getCoordinatorService()
                                                        .getJobMetrics(jobId)
                                                        .toJsonString();
                                    }
                                    return getJobInfoJson(
                                            jobState, jobMetrics, finishedJobDAGInfo.get(jobId));
                                })
                        .collect(JsonArray::new, JsonArray::add, JsonArray::add);

        writeJson(resp, jobs);
    }
}
