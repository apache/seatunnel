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

package org.apache.seatunnel.engine.core.checkpoint;

import lombok.Data;

import java.io.Serializable;

@Data
public class CheckpointCounts implements Serializable {

    private long triggered;
    private long completed;
    private long failed;
    private long inProgress;
    private long restored;

    public void incrementTriggered() {
        triggered++;
    }

    public void incrementCompleted() {
        completed++;
        if (inProgress > 0) {
            inProgress--;
        }
    }

    public void incrementFailed() {
        failed++;
        if (inProgress > 0) {
            inProgress--;
        }
    }

    public void incrementInProgress() {
        inProgress++;
    }

    public void incrementRestored() {
        restored++;
    }
}
