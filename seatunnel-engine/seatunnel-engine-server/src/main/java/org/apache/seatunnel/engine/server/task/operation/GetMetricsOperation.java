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

package org.apache.seatunnel.engine.server.task.operation;

import org.apache.seatunnel.api.common.metrics.RawJobMetrics;
import org.apache.seatunnel.engine.server.metrics.ZetaMetricsCollector;
import org.apache.seatunnel.engine.server.serializable.TaskDataSerializerHook;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;

import static org.apache.seatunnel.api.common.metrics.MetricTags.JOB_ID;

public class GetMetricsOperation extends Operation implements IdentifiedDataSerializable {
    private RawJobMetrics response;
    private Set<Long> runningJobIds;
    /** Optional metric name prefixes for filtering (e.g. "intermediate_queue_"). */
    private String[] metricNamePrefixes;

    public GetMetricsOperation() {}

    public GetMetricsOperation(Set<Long> runningJobIds) {
        this.runningJobIds = runningJobIds;
    }

    public GetMetricsOperation(Set<Long> runningJobIds, String[] metricNamePrefixes) {
        this.runningJobIds = runningJobIds;
        this.metricNamePrefixes = metricNamePrefixes;
    }

    @Override
    public void run() {
        ILogger logger = getLogger();

        Address callerAddress = getCallerAddress();

        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        if (callerAddress == null
                || nodeEngine.getClusterService().getMember(callerAddress) == null) {
            throw new SecurityException("Caller is not a cluster member: " + callerAddress);
        }
        Address masterAddress = getNodeEngine().getMasterAddress();
        if (!callerAddress.equals(masterAddress)) {
            throw new IllegalStateException(
                    "Caller "
                            + callerAddress
                            + " cannot get metrics"
                            + " because it is not master. Master is: "
                            + masterAddress);
        }
        Predicate<MetricDescriptor> metricDescriptorPredicate =
                dis -> {
                    String jobIdStr = dis.tagValue(JOB_ID);
                    if (jobIdStr == null) {
                        return false;
                    }
                    long jobId;
                    try {
                        jobId = Long.parseLong(jobIdStr);
                    } catch (Exception e) {
                        return false;
                    }
                    if (!runningJobIds.contains(jobId)) {
                        return false;
                    }
                    if (metricNamePrefixes == null || metricNamePrefixes.length == 0) {
                        return true;
                    }
                    String metricName = dis.metric();
                    if (metricName == null) {
                        return false;
                    }
                    for (String prefix : metricNamePrefixes) {
                        if (prefix != null && metricName.startsWith(prefix)) {
                            return true;
                        }
                    }
                    return false;
                };

        ZetaMetricsCollector metricsRenderer =
                new ZetaMetricsCollector(
                        metricDescriptorPredicate, nodeEngine.getLocalMember(), logger);
        nodeEngine.getMetricsRegistry().collect(metricsRenderer);
        response = metricsRenderer.getMetrics();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLongArray(runningJobIds.stream().mapToLong(Long::longValue).toArray());
        if (metricNamePrefixes == null || metricNamePrefixes.length == 0) {
            out.writeInt(0);
            return;
        }
        out.writeInt(metricNamePrefixes.length);
        for (String p : metricNamePrefixes) {
            out.writeString(p);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        this.runningJobIds =
                Arrays.stream(Objects.requireNonNull(in.readLongArray()))
                        .collect(HashSet::new, HashSet::add, HashSet::addAll);
        // Backward compatible: older versions may not have these fields.
        try {
            int n = in.readInt();
            if (n > 0) {
                this.metricNamePrefixes = new String[n];
                for (int i = 0; i < n; i++) {
                    this.metricNamePrefixes[i] = in.readString();
                }
            }
        } catch (IOException ignored) {
            // ignore
        }
    }

    @Override
    public Object getResponse() {
        return response;
    }

    @Override
    public int getFactoryId() {
        return TaskDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return TaskDataSerializerHook.GET_METRICS_OPERATION;
    }
}
