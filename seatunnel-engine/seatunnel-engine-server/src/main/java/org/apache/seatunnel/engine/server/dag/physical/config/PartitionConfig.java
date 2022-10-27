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

public class PartitionConfig implements FlowConfig {

    private final int partitionCount;

    private final int targetCount;

    private final int parallelismIndex;

    public PartitionConfig(int partitionCount, int targetCount, int parallelismIndex) {
        this.partitionCount = partitionCount;
        this.targetCount = targetCount;
        this.parallelismIndex = parallelismIndex;
    }

    public int getPartitionCount() {
        return partitionCount;
    }

    public int getTargetCount() {
        return targetCount;
    }

    public int getParallelismIndex() {
        return parallelismIndex;
    }
}
