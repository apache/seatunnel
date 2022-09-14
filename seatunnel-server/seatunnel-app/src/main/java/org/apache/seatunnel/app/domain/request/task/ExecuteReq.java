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

package org.apache.seatunnel.app.domain.request.task;

import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

import javax.validation.constraints.NotEmpty;

import java.util.Date;
import java.util.Map;

@Data
public class ExecuteReq {
    @ApiModelProperty(value = "script id", required = true, dataType = "Integer", hidden = true)
    private Integer scriptId;
    @ApiModelProperty(value = "execute content", required = true, dataType = "String")
    private String content;
    @ApiModelProperty(value = "operator id", required = true, dataType = "Integer", hidden = true)
    private Integer operatorId;
    @ApiModelProperty(value = "script params", required = true, dataType = "Map")
    @NotEmpty
    private Map<String, Object> params;
    @ApiModelProperty(value = "execute type", required = true, dataType = "Integer", allowableValues = "0, 1, 2, 3")
    private Integer executeType;
    @ApiModelProperty(value = "start time", required = false, dataType = "Date", hidden = true)
    private Date startTime = new Date();
    @ApiModelProperty(value = "end time", required = false, dataType = "Date", hidden = true)
    private Date endTime = new Date();
    @ApiModelProperty(value = "parallelism number", required = false, dataType = "Integer", hidden = true)
    private Integer parallelismNum = 1;
}
