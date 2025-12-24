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

package org.apache.seatunnel.connectors.doris.rest.models;

import org.apache.seatunnel.shade.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.seatunnel.shade.com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
@JsonIgnoreProperties(ignoreUnknown = true)
public class RespContent {

    @JsonProperty(value = "TxnId")
    private long txnId;

    @JsonProperty(value = "Label")
    private String label;

    @JsonProperty(value = "Status")
    private String status;

    @JsonProperty(value = "TwoPhaseCommit")
    private String twoPhaseCommit;

    @JsonProperty(value = "ExistingJobStatus")
    private String existingJobStatus;

    @JsonProperty(value = "Message")
    private String message;

    @JsonProperty(value = "ErrorURL")
    private String errorURL;
}
