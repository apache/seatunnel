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

import org.apache.seatunnel.shade.com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import lombok.NoArgsConstructor;
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
    @NoArgsConstructor
    public static class StreamLoadResponseBody implements Serializable {
        @JsonProperty("TxnId")
        @SuppressWarnings("checkstyle:MemberName")
        private Long txnId;

        @JsonProperty("Label")
        @SuppressWarnings("checkstyle:MemberName")
        private String label;

        @JsonProperty("State")
        @SuppressWarnings("checkstyle:MemberName")
        private String state;

        @JsonProperty("Status")
        @SuppressWarnings("checkstyle:MemberName")
        private String status;

        @JsonProperty("ExistingJobStatus")
        @SuppressWarnings("checkstyle:MemberName")
        private String existingJobStatus;

        @JsonProperty("Message")
        @SuppressWarnings("checkstyle:MemberName")
        private String message;

        @JsonProperty("Msg")
        @SuppressWarnings("checkstyle:MemberName")
        private String msg;

        @JsonProperty("NumberTotalRows")
        @SuppressWarnings("checkstyle:MemberName")
        private Long numberTotalRows;

        @JsonProperty("NumberLoadedRows")
        @SuppressWarnings("checkstyle:MemberName")
        private Long numberLoadedRows;

        @JsonProperty("NumberFilteredRows")
        @SuppressWarnings("checkstyle:MemberName")
        private Long numberFilteredRows;

        @JsonProperty("NumberUnselectedRows")
        @SuppressWarnings("checkstyle:MemberName")
        private Long numberUnselectedRows;

        @JsonProperty("ErrorURL")
        @SuppressWarnings("checkstyle:MemberName")
        private String errorURL;

        @JsonProperty("LoadBytes")
        @SuppressWarnings("checkstyle:MemberName")
        private Long loadBytes;

        @JsonProperty("LoadTimeMs")
        @SuppressWarnings("checkstyle:MemberName")
        private Long loadTimeMs;

        @JsonProperty("BeginTxnTimeMs")
        @SuppressWarnings("checkstyle:MemberName")
        private Long beginTxnTimeMs;

        @JsonProperty("StreamLoadPlanTimeMs")
        @SuppressWarnings("checkstyle:MemberName")
        private Long streamLoadPlanTimeMs;

        @JsonProperty("ReceivedDataTimeMs")
        @SuppressWarnings("checkstyle:MemberName")
        private Long readDataTimeMs;

        @JsonProperty("WriteDataTimeMs")
        @SuppressWarnings("checkstyle:MemberName")
        private Long writeDataTimeMs;

        @JsonProperty("CommitAndPublishTimeMs")
        @SuppressWarnings("checkstyle:MemberName")
        private Long commitAndPublishTimeMs;
    }
}
