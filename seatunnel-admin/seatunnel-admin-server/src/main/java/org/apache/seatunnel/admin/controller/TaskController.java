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

import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.apache.seatunnel.admin.common.Result;
import org.apache.seatunnel.admin.dto.TaskPage;
import org.apache.seatunnel.admin.entity.StTask;
import org.apache.seatunnel.admin.service.IStTaskService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@Api(tags = "TASKS_TAG")
@RestController
@RequestMapping("/tasks")
public class TaskController {

    private static final Logger logger = LoggerFactory.getLogger(TaskController.class);

    @Autowired
    private IStTaskService taskService;

    @ApiOperation(value = "queryTask", notes = "QUERY_TASK_NOTES")
    @GetMapping(value = "/list")
    public Result<List<StTask>> queryPageList(TaskPage taskPage) {
        QueryWrapper<StTask> queryWrapper = new QueryWrapper<>();
        queryWrapper.eq(StrUtil.isNotBlank(taskPage.getName()), "name", taskPage.getName());
        Page<StTask> page = new Page<>(taskPage.getPageNo(), taskPage.getPageSize());
        taskService.page(page, queryWrapper);
        return Result.success(page.getRecords());
    }

    @ApiOperation(value = "createTask", notes = "CREATE_TASKS_NOTES")
    @PostMapping(value = "/create")
    public Result createData(@RequestBody StTask stTask) {
        // TODO:
        return Result.success("");
    }

    @ApiOperation(value = "updateTask", notes = "UPDATE_TASKS_NOTES")
    @PostMapping(value = "/update")
    public Result updateData(@RequestBody StTask stTask) {
        // TODO:
        return Result.success("");
    }

    @ApiOperation(value = "delTaskById", notes = "DELETE_TASKS_BY_ID_NOTES")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "id", value = "TASK_ID", dataType = "Long", example = "100")
    })
    @PostMapping(value = "/delete")
    public Result delById(@RequestParam(value = "id") Long id) {
        // TODO:
        return Result.success("");
    }
}
