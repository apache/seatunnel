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

package org.apache.seatunnel.connectors.seatunnel.starrocks.client;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
public class StreamLoadResponse implements Serializable {

    private boolean cancel;

    private Long flushRows;
    private Long flushBytes;
    private Long costNanoTime;

    private StreamLoadResponseBody body;
    private Exception exception;

    public void cancel() {
        this.cancel = true;
    }

    @Setter
    @Getter
    public static class StreamLoadResponseBody implements Serializable {
        private Long txnId;
        private String label;
        private String state;
        private String status;
        private String existingJobStatus;
        private String message;
        private String msg;
        private Long numberTotalRows;
        private Long numberLoadedRows;
        private Long numberFilteredRows;
        private Long numberUnselectedRows;
        private String errorURL;
        private Long loadBytes;
        private Long loadTimeMs;
        private Long beginTxnTimeMs;
        private Long streamLoadPlanTimeMs;
        private Long readDataTimeMs;
        private Long writeDataTimeMs;
        private Long commitAndPublishTimeMs;
    }
}
