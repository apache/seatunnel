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

package org.apache.seatunnel.app.domain.response;

import io.swagger.annotations.ApiModel;

import java.util.List;

@ApiModel(value = "pageInfo", description = "page info")
@SuppressWarnings("MagicNumber")
public class PageInfo<T> {
    private List<T> data;
    private Integer totalCount = 0;
    private Integer totalPage = 0;
    private Integer pageNo = 1;
    private Integer pageSize = 20;

    public List<T> getData() {
        return data;
    }

    public void setData(List<T> data) {
        this.data = data;
    }

    public Integer getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(Integer totalCount) {
        this.totalCount = totalCount;

        if (pageSize == null || pageSize == 0) {
            pageSize = 20;
        }
        if (this.totalCount % this.pageSize == 0) {
            this.totalPage = this.totalCount / this.pageSize == 0 ? 1 : this.totalCount / this.pageSize;
            return;
        }
        this.totalPage = this.totalCount / this.pageSize + 1;
    }

    public Integer getTotalPage() {
        return totalPage;
    }

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
