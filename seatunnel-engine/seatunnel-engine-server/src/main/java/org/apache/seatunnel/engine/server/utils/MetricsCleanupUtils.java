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

package org.apache.seatunnel.engine.server.utils;

import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.server.dag.physical.PipelineLocation;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.metrics.MetricsCleanupScheduler;
import org.apache.seatunnel.engine.server.metrics.SeaTunnelMetricsContext;

import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.map.IMap;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
public class MetricsCleanupUtils {
    private MetricsCleanupUtils() {}

    public static void cleanupMetrics(
            PipelineLocation pipelineLocation,
            IMap<Long, HashMap<TaskLocation, SeaTunnelMetricsContext>> metricsImap,
            MetricsCleanupScheduler metricsCleanupScheduler) {
        boolean lockedIMap = false;
        try {
            lockedIMap =
                    metricsImap.tryLock(Constant.IMAP_RUNNING_JOB_METRICS_KEY, 5, TimeUnit.SECONDS);
            if (!lockedIMap) {
                log.warn("lock imap failed in update metrics");
                boolean offer = metricsCleanupScheduler.offerRetryQueue(pipelineLocation);
                if (!offer) {
                    log.warn("failed to add pipelineLocation to retry queue");
                }
                return;
            }

            HashMap<TaskLocation, SeaTunnelMetricsContext> centralMap =
                    metricsImap.get(Constant.IMAP_RUNNING_JOB_METRICS_KEY);
            if (centralMap != null) {
                List<TaskLocation> collect =
                        centralMap.keySet().stream()
                                .filter(
                                        taskLocation ->
                                                taskLocation
                                                        .getTaskGroupLocation()
                                                        .getPipelineLocation()
                                                        .equals(pipelineLocation))
                                .collect(Collectors.toList());
                collect.forEach(centralMap::remove);
                metricsImap.put(Constant.IMAP_RUNNING_JOB_METRICS_KEY, centralMap);
            }
        } catch (Exception e) {
            log.warn("failed to remove metrics context", e);
        } finally {
            if (lockedIMap) {
                boolean unLockedIMap = false;
                while (!unLockedIMap) {
                    try {
                        metricsImap.unlock(Constant.IMAP_RUNNING_JOB_METRICS_KEY);
                        unLockedIMap = true;
                    } catch (OperationTimeoutException e) {
                        log.warn("unlock imap failed in update metrics", e);
                    }
                }
            }
        }
    }
}
