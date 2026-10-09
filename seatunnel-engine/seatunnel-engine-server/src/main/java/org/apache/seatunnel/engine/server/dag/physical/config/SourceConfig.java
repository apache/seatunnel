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

package org.apache.seatunnel.engine.server.dag.physical.config;

import org.apache.seatunnel.engine.common.runtime.source.ManagedSourceRuntimeMode;
import org.apache.seatunnel.engine.server.execution.TaskLocation;

public class SourceConfig implements FlowConfig {

    private TaskLocation enumeratorTask;
    private ManagedSourceRuntimeMode runtimeMode = ManagedSourceRuntimeMode.LEGACY;
    private int runtimeProtocolVersion;
    private int connectorStateVersion;
    private String capabilityDigest = "";
    private boolean checkpointEnabled = true;

    public TaskLocation getEnumeratorTask() {
        return enumeratorTask;
    }

    public void setEnumeratorTask(TaskLocation enumeratorTask) {
        this.enumeratorTask = enumeratorTask;
    }

    public ManagedSourceRuntimeMode getRuntimeMode() {
        // New fields in an older serialized physical plan are restored as JVM defaults.
        return runtimeMode == null ? ManagedSourceRuntimeMode.LEGACY : runtimeMode;
    }

    public void setRuntimeMode(ManagedSourceRuntimeMode runtimeMode) {
        this.runtimeMode = runtimeMode;
    }

    public int getRuntimeProtocolVersion() {
        return runtimeProtocolVersion;
    }

    public void setRuntimeProtocolVersion(int runtimeProtocolVersion) {
        this.runtimeProtocolVersion = runtimeProtocolVersion;
    }

    public int getConnectorStateVersion() {
        return connectorStateVersion;
    }

    public void setConnectorStateVersion(int connectorStateVersion) {
        this.connectorStateVersion = connectorStateVersion;
    }

    public String getCapabilityDigest() {
        return capabilityDigest == null ? "" : capabilityDigest;
    }

    public void setCapabilityDigest(String capabilityDigest) {
        this.capabilityDigest = capabilityDigest;
    }

    public boolean isCheckpointEnabled() {
        return checkpointEnabled;
    }

    public void setCheckpointEnabled(boolean checkpointEnabled) {
        this.checkpointEnabled = checkpointEnabled;
    }
}
