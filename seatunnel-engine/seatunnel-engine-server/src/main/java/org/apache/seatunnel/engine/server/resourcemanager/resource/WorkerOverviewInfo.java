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

package org.apache.seatunnel.engine.server.resourcemanager.resource;

import lombok.Data;

import java.io.Serializable;
import java.util.Map;

/**
 * Read-only, per-worker projection of the resource manager's live {@link
 * org.apache.seatunnel.engine.server.resourcemanager.worker.WorkerProfile} state for the Web UI.
 * Every field is derived from state the resource manager already tracks for scheduling; this class
 * adds no new persisted or mutable state of its own.
 */
@Data
public class WorkerOverviewInfo implements Serializable {
    private String host;
    private int port;
    private int totalSlot;
    private int usedSlot;
    private boolean dynamicSlot;
    private Double cpuPercentage;
    private Double memPercentage;
    private Map<String, String> attributes;
}
