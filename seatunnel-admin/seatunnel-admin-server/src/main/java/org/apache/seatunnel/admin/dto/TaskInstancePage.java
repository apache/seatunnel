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

package org.apache.seatunnel.admin.dto;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

@ApiModel(value = "TaskInstancePage")
public class TaskInstancePage extends BasePage {

    @ApiModelProperty(name = "name", value = "TASK_INSTANCE_FIELD_NAME")
    private String name;

    @ApiModelProperty(name = "type", value = "TASK_INSTANCE_FIELD_TYPE")
    private String type;

    @ApiModelProperty(name = "instanceStatus", value = "TASK_INSTANCE_FIELD_STATUS", example = "1")
    private Integer instanceStatus;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Integer getInstanceStatus() {
        return instanceStatus;
    }

    public void setInstanceStatus(Integer instanceStatus) {
        this.instanceStatus = instanceStatus;
    }
}
