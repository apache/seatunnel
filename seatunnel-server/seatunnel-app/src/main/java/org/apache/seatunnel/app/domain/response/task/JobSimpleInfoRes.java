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

@ApiModel(value = "jobSimpleInfoRes", description = "job simple information")
@Data
@Builder
public class JobSimpleInfoRes {
    @ApiModelProperty(value = "job id", dataType = "Long")
    private long jobId;
    @ApiModelProperty(value = "job status", dataType = "String")
    private String jobStatus;
    @ApiModelProperty(value = "job creator", dataType = "String")
    private String creatorName;
    @ApiModelProperty(value = "job mender", dataType = "String")
    private String menderName;
    @ApiModelProperty(value = "job create time", dataType = "String")
    private Date createTime;
    @ApiModelProperty(value = "job update time", dataType = "String")
    private Date updateTime;
}
