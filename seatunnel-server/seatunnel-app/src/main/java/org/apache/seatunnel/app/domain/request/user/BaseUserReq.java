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

package org.apache.seatunnel.app.domain.request.user;

import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

import javax.validation.constraints.NotNull;

@Data
public class BaseUserReq {
    @ApiModelProperty(value = "user name", required = true, dataType = "String")
    private String username;
    @ApiModelProperty(value = "user password", required = true, dataType = "String")
    private String password;
    @ApiModelProperty(value = "user status", required = true, dataType = "Byte")
    @NotNull
    private Byte status;
    @ApiModelProperty(value = "user type", required = true, dataType = "Byte")
    @NotNull
    private Byte type;
}
