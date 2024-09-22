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

import org.apache.seatunnel.common.utils.DateTimeUtils;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.core.classloader.ClassLoaderService;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.core.job.JobInfo;
import org.apache.seatunnel.engine.core.job.JobStatus;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.operation.GetJobMetricsOperation;
import org.apache.seatunnel.engine.server.operation.GetJobStatusOperation;
import org.apache.seatunnel.engine.server.rest.RestConstant;
import org.apache.seatunnel.engine.server.utils.NodeEngineUtil;

import com.google.gson.Gson;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.jet.impl.execution.init.CustomClassLoadedObject;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.util.Map;

import static com.hazelcast.internal.util.JsonUtil.toJsonObject;
import static org.apache.seatunnel.engine.server.rest.RestHttpGetCommandProcessor.getJobMetrics;

public class BaseServlet extends HttpServlet {

    protected final HazelcastInstanceImpl hazelcastInstance;

    public BaseServlet(HazelcastInstanceImpl hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
    }

    protected void writeJson(HttpServletResponse resp, Object obj) throws IOException {
        resp.setContentType("application/json");
        resp.getWriter().write(new Gson().toJson(obj));
    }

    protected void writeJson(HttpServletResponse resp, JsonArray jsonArray) throws IOException {
        resp.setContentType("application/json");
        resp.getWriter().write(jsonArray.toString());
    }

    protected void writeJson(HttpServletResponse resp, JsonObject jsonObject) throws IOException {
        resp.setContentType("application/json");
        resp.getWriter().write(jsonObject.toString());
    }

    protected void writeJson(HttpServletResponse resp, JsonArray jsonArray, int statusCode)
            throws IOException {
        resp.setContentType("application/json");
        resp.setStatus(statusCode);
        resp.getWriter().write(jsonArray.toString());
    }

    protected void writeJson(HttpServletResponse resp, JsonObject jsonObject, int statusCode)
            throws IOException {
        resp.setContentType("application/json");
        resp.setStatus(statusCode);
        resp.getWriter().write(jsonObject.toString());
    }

    protected void writeJson(HttpServletResponse resp, Object obj, int statusCode)
            throws IOException {
        resp.setContentType("application/json");
        resp.setStatus(statusCode);
        resp.getWriter().write(new Gson().toJson(obj));
    }

    protected JsonObject convertToJson(JobInfo jobInfo, long jobId) {

        JsonObject jobInfoJson = new JsonObject();
        JobImmutableInformation jobImmutableInformation =
                hazelcastInstance
                        .node
                        .getNodeEngine()
                        .getSerializationService()
                        .toObject(
                                hazelcastInstance
                                        .node
                                        .getNodeEngine()
                                        .getSerializationService()
                                        .toObject(jobInfo.getJobImmutableInformation()));

        SeaTunnelServer seaTunnelServer = getSeaTunnelServer(true);
        ClassLoaderService classLoaderService =
                seaTunnelServer == null
                        ? getSeaTunnelServer(false).getClassLoaderService()
                        : seaTunnelServer.getClassLoaderService();
        ClassLoader classLoader =
                classLoaderService.getClassLoader(
                        jobId, jobImmutableInformation.getPluginJarsUrls());
        LogicalDag logicalDag =
                CustomClassLoadedObject.deserializeWithCustomClassLoader(
                        hazelcastInstance.node.getNodeEngine().getSerializationService(),
                        classLoader,
                        jobImmutableInformation.getLogicalDag());
        classLoaderService.releaseClassLoader(jobId, jobImmutableInformation.getPluginJarsUrls());

        String jobMetrics;
        JobStatus jobStatus;
        if (seaTunnelServer == null) {
            jobMetrics =
                    (String)
                            NodeEngineUtil.sendOperationToMasterNode(
                                            hazelcastInstance.node.getNodeEngine(),
                                            new GetJobMetricsOperation(jobId))
                                    .join();
            jobStatus =
                    JobStatus.values()[
                            (int)
                                    NodeEngineUtil.sendOperationToMasterNode(
                                                    hazelcastInstance.node.getNodeEngine(),
                                                    new GetJobStatusOperation(jobId))
                                            .join()];
        } else {
            jobMetrics =
                    seaTunnelServer.getCoordinatorService().getJobMetrics(jobId).toJsonString();
            jobStatus = seaTunnelServer.getCoordinatorService().getJobStatus(jobId);
        }

        jobInfoJson
                .add(RestConstant.JOB_ID, String.valueOf(jobId))
                .add(RestConstant.JOB_NAME, logicalDag.getJobConfig().getName())
                .add(RestConstant.JOB_STATUS, jobStatus.toString())
                .add(
                        RestConstant.ENV_OPTIONS,
                        toJsonObject(logicalDag.getJobConfig().getEnvOptions()))
                .add(
                        RestConstant.CREATE_TIME,
                        DateTimeUtils.toString(
                                jobImmutableInformation.getCreateTime(),
                                DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS))
                .add(RestConstant.JOB_DAG, logicalDag.getLogicalDagAsJson())
                .add(
                        RestConstant.PLUGIN_JARS_URLS,
                        (JsonValue)
                                jobImmutableInformation.getPluginJarsUrls().stream()
                                        .map(
                                                url -> {
                                                    JsonObject jarUrl = new JsonObject();
                                                    jarUrl.add(
                                                            RestConstant.JAR_PATH, url.toString());
                                                    return jarUrl;
                                                })
                                        .collect(JsonArray::new, JsonArray::add, JsonArray::add))
                .add(
                        RestConstant.IS_START_WITH_SAVE_POINT,
                        jobImmutableInformation.isStartWithSavePoint())
                .add(RestConstant.METRICS, toJsonObject(getJobMetrics(jobMetrics)));

        return jobInfoJson;
    }

    protected SeaTunnelServer getSeaTunnelServer(boolean shouldBeMaster) {
        Map<String, Object> extensionServices =
                hazelcastInstance.node.getNodeExtension().createExtensionServices();
        SeaTunnelServer seaTunnelServer =
                (SeaTunnelServer) extensionServices.get(Constant.SEATUNNEL_SERVICE_NAME);
        if (shouldBeMaster && !seaTunnelServer.isMasterNode()) {
            return null;
        }
        return seaTunnelServer;
    }
}
