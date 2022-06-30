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

package org.apache.seatunnel.admin.controller;

import org.apache.seatunnel.admin.common.Result;
import org.apache.seatunnel.admin.dto.DataSourcePage;
import org.apache.seatunnel.admin.entity.StDatasource;
import org.apache.seatunnel.admin.service.IStDatasourceService;
import org.apache.seatunnel.admin.utils.StringUtils;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@Api(tags = "DATA_SOURCES_TAG")
@RestController
@RequestMapping("/datasources")
public class DatasourceController {

    private static final Logger LOGGER = LoggerFactory.getLogger(DatasourceController.class);

    @Autowired
    private IStDatasourceService datasourceService;

    @ApiOperation(value = "queryDatasource", notes = "QUERY_DATA_SOURCES_NOTES")
    @GetMapping(value = "/list")
    public Result<List<StDatasource>> queryPageList(DataSourcePage dataSourcePage) {
        QueryWrapper<StDatasource> queryWrapper = new QueryWrapper<>();
        queryWrapper.eq(StringUtils.isNotBlank(dataSourcePage.getName()), "name", dataSourcePage.getName());
        Page<StDatasource> page = new Page<>(dataSourcePage.getPageNo(), dataSourcePage.getPageSize());
        datasourceService.page(page, queryWrapper);
        return Result.success(page.getRecords());
    }

    @ApiOperation(value = "createDatasource", notes = "CREATE_DATA_SOURCES_NOTES")
    @PostMapping(value = "/create")
    public Result createData(@RequestBody StDatasource datasource) {
        // TODO:
        return Result.success("");
    }

    @ApiOperation(value = "updateDatasource", notes = "UPDATE_DATA_SOURCES_NOTES")
    @PostMapping(value = "/update")
    public Result updateData(@RequestBody StDatasource datasource) {
        // TODO:
        return Result.success("");
    }

    @ApiOperation(value = "testConnectDatasource", notes = "TEST_CONNECT_DATA_SOURCES_NOTES")
    @PostMapping(value = "/testConnect")
    public Result testConnect(@RequestBody StDatasource datasource) {
        // TODO:
        return Result.success("");
    }

    @ApiOperation(value = "delDatasourceById", notes = "DELETE_DATA_SOURCES_BY_ID_NOTES")
    @ApiImplicitParams({ @ApiImplicitParam(name = "id", value = "DATA_SOURCE_ID", dataType = "Long", example = "100") })
    @PostMapping(value = "/delete")
    public Result delById(@RequestParam(value = "id") Long id) {
        // TODO:
        return Result.success("");
    }
}
