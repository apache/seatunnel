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

package org.apache.seatunnel.app.domain.response.script;

import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

import java.util.Date;

@Data
public class BaseScriptInfoRes {
    @ApiModelProperty(value = "script id", dataType = "int")
    private int id;
    @ApiModelProperty(value = "script name", dataType = "String")
    private String name;
    @ApiModelProperty(value = "script status", dataType = "type")
    private byte status;
    @ApiModelProperty(value = "script type", dataType = "type")
    private byte type;
    @ApiModelProperty(value = "script creator id", required = true, dataType = "Integer")
    private Integer creatorId;
    @ApiModelProperty(value = "script mender id", required = true, dataType = "Integer")
    private Integer menderId;
    @ApiModelProperty(value = "script create time", dataType = "Date")
    private Date createTime;
    @ApiModelProperty(value = "script update time", dataType = "Date")
    private Date updateTime;
}
