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
import org.apache.seatunnel.admin.dto.TaskInstancePage;
import org.apache.seatunnel.admin.entity.StTaskInstance;
import org.apache.seatunnel.admin.service.IStTaskInstanceService;
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
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@Api(tags = "TASK_INSTANCE_TAG")
@RestController
@RequestMapping("/task/instance")
public class TaskInstanceController {

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskInstanceController.class);

    @Autowired
    private IStTaskInstanceService taskInstanceService;

    @ApiOperation(value = "queryTaskInstance", notes = "QUERY_TASK_INSTANCE_NOTES")
    @GetMapping(value = "/list")
    public Result<List<StTaskInstance>> queryPageList(TaskInstancePage taskInstancePage) {
        QueryWrapper<StTaskInstance> queryWrapper = new QueryWrapper<>();
        queryWrapper
                .like(StringUtils.isNotBlank(taskInstancePage.getName()), "name", taskInstancePage.getName())
                .eq(StringUtils.isNotBlank(taskInstancePage.getType()), "type", taskInstancePage.getType())
                .eq(taskInstancePage.getInstanceStatus() != null && taskInstancePage.getInstanceStatus() > 0, "instance_status", taskInstancePage.getInstanceStatus());
        Page<StTaskInstance> page = new Page<>(taskInstancePage.getPageNo(), taskInstancePage.getPageSize());
        taskInstanceService.page(page, queryWrapper);
        return Result.success(page.getRecords());
    }

    @ApiOperation(value = "delTaskInstanceById", notes = "DELETE_TASK_INSTANCE_BY_ID_NOTES")
    @ApiImplicitParams({ @ApiImplicitParam(name = "id", value = "TASK_INSTANCE_ID", dataType = "Long", example = "100") })
    @PostMapping(value = "/delete")
    public Result delById(@RequestParam(value = "id") Long id) {
        // TODO:
        return Result.success("");
    }
}
