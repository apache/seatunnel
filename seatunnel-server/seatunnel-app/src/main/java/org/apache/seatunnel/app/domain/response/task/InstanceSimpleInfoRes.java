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

package org.apache.seatunnel.app.domain.response.task;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Builder;
import lombok.Data;

import java.util.Date;

@Data
@Builder
@ApiModel(value = "instanceSimpleInfoRes", description = "instance simple information")
public class InstanceSimpleInfoRes {
    @ApiModelProperty(value = "instance id", dataType = "Long")
    private long instanceId;
    @ApiModelProperty(value = "job id", dataType = "Long")
    private long jobId;
    @ApiModelProperty(value = "instance name", dataType = "String")
    private String instanceName;
    @ApiModelProperty(value = "submit time", dataType = "Date")
    private Date submitTime;
    @ApiModelProperty(value = "start time", dataType = "Date")
    private Date startTime;
    @ApiModelProperty(value = "end time", dataType = "Date")
    private Date endTime;
    @ApiModelProperty(value = "task status", dataType = "String")
    private String status;
    @ApiModelProperty(value = "execution duration", dataType = "String")
    private String executionDuration;
    @ApiModelProperty(value = "retry times", dataType = "Long")
    private long retryTimes;
}
