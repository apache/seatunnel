/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.engine.server.diagnostic;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Read-only projection of the latest resource-manager heartbeat for one worker.
 *
 * <p>For dynamic-slot workers, {@code totalSlots} is the number of currently tracked assigned and
 * unassigned slots rather than a fixed capacity. Callers should use {@code dynamicSlot} to
 * interpret the slot fields.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class WorkerResourceDiagnostic implements Serializable {
    // Preserve compatibility with diagnostics serialized before resource fields were added.
    private static final long serialVersionUID = 1347570277332453777L;

    private String address;
    private Map<String, String> tags;
    private int totalSlots;
    private int freeSlots;
    private int usedSlots;
    private boolean dynamicSlot;
    private Integer totalCpuCores;
    private Integer availableCpuCores;
    private Long totalHeapMemoryBytes;
    private Long availableHeapMemoryBytes;
    private Double cpuUsage;
    private Double memUsage;
    private List<Long> runningJobIds;
}
