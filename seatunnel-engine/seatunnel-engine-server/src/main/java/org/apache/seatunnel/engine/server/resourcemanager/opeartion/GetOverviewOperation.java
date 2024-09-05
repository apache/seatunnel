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

package org.apache.seatunnel.engine.server.resourcemanager.opeartion;

import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.core.job.JobStatus;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.master.JobHistoryService.JobState;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManager;
import org.apache.seatunnel.engine.server.resourcemanager.resource.OverviewInfo;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;
import org.apache.seatunnel.engine.server.serializable.ResourceDataSerializerHook;

import com.hazelcast.map.IMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.Operation;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@Slf4j
public class GetOverviewOperation extends Operation implements IdentifiedDataSerializable {

    private OverviewInfo overviewInfo;
    private Map<String, String> tags;

    public GetOverviewOperation() {}

    public GetOverviewOperation(Map<String, String> tags) {
        this.tags = tags;
    }

    @Override
    public void run() throws Exception {
        SeaTunnelServer server = getService();
        overviewInfo = getOverviewInfo(server, getNodeEngine(), tags);
    }

    @Override
    public Object getResponse() {
        return overviewInfo;
    }

    @Override
    public int getFactoryId() {
        return ResourceDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return ResourceDataSerializerHook.REQUEST_SLOT_INFO_TYPE;
    }

    @Override
    public String getServiceName() {
        return SeaTunnelServer.SERVICE_NAME;
    }

    public static OverviewInfo getOverviewInfo(
            SeaTunnelServer server, NodeEngine nodeEngine, Map<String, String> tags) {
        OverviewInfo overviewInfo = new OverviewInfo();
        ResourceManager resourceManager = server.getCoordinatorService().getResourceManager();

        List<SlotProfile> assignedSlots = resourceManager.getAssignedSlots(tags);

        List<SlotProfile> unassignedSlots = resourceManager.getUnassignedSlots(tags);
        IMap<Long, JobState> finishedJob =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_FINISHED_JOB_STATE);
        overviewInfo.setTotalSlot(assignedSlots.size() + unassignedSlots.size());
        overviewInfo.setUnassignedSlot(unassignedSlots.size());
        overviewInfo.setWorkers(resourceManager.workerCount(tags));
        overviewInfo.setRunningJobs(
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_INFO).size());
        overviewInfo.setFailedJobs(
                finishedJob.values().stream()
                        .filter(
                                jobState ->
                                        jobState.getJobStatus()
                                                .name()
                                                .equals(JobStatus.FAILED.toString()))
                        .count());
        overviewInfo.setCancelledJobs(
                finishedJob.values().stream()
                        .filter(
                                jobState ->
                                        jobState.getJobStatus()
                                                .name()
                                                .equals(JobStatus.CANCELED.toString()))
                        .count());
        overviewInfo.setFinishedJobs(
                finishedJob.values().stream()
                        .filter(
                                jobState ->
                                        jobState.getJobStatus()
                                                .name()
                                                .equals(JobStatus.FINISHED.toString()))
                        .count());

        return overviewInfo;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(tags);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        tags = in.readObject();
    }
}
