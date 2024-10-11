/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.sls.source;

import org.apache.seatunnel.api.source.SourceSplit;

import lombok.Getter;
import lombok.Setter;

public class SlsSourceSplit implements SourceSplit {

    @Getter private String project;
    @Getter private String logStore;
    @Getter private String consumer;
    @Getter private Integer shardId;
    @Getter private String startCursor;
    @Getter private Integer fetchSize;
    @Setter @Getter private transient volatile boolean finish = false;

    SlsSourceSplit(
            String project,
            String logStore,
            String consumer,
            Integer shardId,
            String startCursor,
            Integer fetchSize) {
        this.project = project;
        this.logStore = logStore;
        this.consumer = consumer;
        this.shardId = shardId;
        this.startCursor = startCursor;
        this.fetchSize = fetchSize;
    }

    @Override
    public String splitId() {
        return String.valueOf(shardId);
    }

    public void setStartCursor(String cursor) {
        this.startCursor = cursor;
    }

    public SlsSourceSplit copy() {
        return new SlsSourceSplit(
                this.project,
                this.logStore,
                this.consumer,
                this.shardId,
                this.startCursor,
                this.fetchSize);
    }
}
