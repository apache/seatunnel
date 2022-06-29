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
import org.apache.seatunnel.admin.dto.HostPage;
import org.apache.seatunnel.admin.entity.StHost;
import org.apache.seatunnel.admin.service.IStHostService;

import cn.hutool.core.util.StrUtil;
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

@Api(tags = "HOST_TAG")
@RestController
@RequestMapping("/hosts")
public class HostController {

    private static final Logger LOGGER = LoggerFactory.getLogger(HostController.class);

    @Autowired
    private IStHostService hostService;

    @ApiOperation(value = "queryHost", notes = "QUERY_HOST_NOTES")
    @GetMapping(value = "/list")
    public Result<List<StHost>> queryPageList(HostPage hostPage) {
        QueryWrapper<StHost> queryWrapper = new QueryWrapper<>();
        queryWrapper.eq(StrUtil.isNotBlank(hostPage.getName()), "name", hostPage.getName());
        Page<StHost> page = new Page<>(hostPage.getPageNo(), hostPage.getPageSize());
        hostService.page(page, queryWrapper);
        return Result.success(page.getRecords());
    }

    @ApiOperation(value = "createHost", notes = "CREATE_HOST_NOTES")
    @PostMapping(value = "/create")
    public Result createData(@RequestBody StHost stHost) {
        // TODO:
        return Result.success("");
    }

    @ApiOperation(value = "updateHost", notes = "UPDATE_HOST_NOTES")
    @PostMapping(value = "/update")
    public Result updateData(@RequestBody StHost stHost) {
        // TODO:
        return Result.success("");
    }

    @ApiOperation(value = "testConnectHost", notes = "TEST_CONNECT_HOST_NOTES")
    @PostMapping(value = "/testConnect")
    public Result testConnect(@RequestBody StHost stHost) {
        // TODO:
        return Result.success("");
    }

    @ApiOperation(value = "delHostById", notes = "DELETE_HOST_BY_ID_NOTES")
    @ApiImplicitParams({ @ApiImplicitParam(name = "id", value = "HOST_ID", dataType = "Long", example = "100") })
    @PostMapping(value = "/delete")
    public Result delById(@RequestParam(value = "id") Long id) {
        // TODO:
        return Result.success("");
    }
}
