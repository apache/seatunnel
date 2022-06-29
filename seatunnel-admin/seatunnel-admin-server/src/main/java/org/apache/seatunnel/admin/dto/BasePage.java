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

import org.apache.seatunnel.admin.common.Constants;

import io.swagger.annotations.ApiModelProperty;

import java.io.Serializable;

public class BasePage implements Serializable {

    @ApiModelProperty(name = "pageNo", value = "PAGENO", dataType = "Int", example = "1")
    private Integer pageNo = Constants.PAGE_NUMBER_DEFAULT_VALUE;
    @ApiModelProperty(name = "pageSize", value = "PAGESIZE", dataType = "Int", example = "10")
    private Integer pageSize = Constants.PAGE_SIZE_DEFAULT_VALUE;

    public Integer getPageNo() {
        return pageNo;
    }

    public void setPageNo(Integer pageNo) {
        this.pageNo = pageNo;
    }

    public Integer getPageSize() {
        return pageSize;
    }

    public void setPageSize(Integer pageSize) {
        this.pageSize = pageSize;
    }
}
