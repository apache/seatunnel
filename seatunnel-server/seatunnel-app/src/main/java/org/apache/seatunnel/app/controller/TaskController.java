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

package org.apache.seatunnel.app.controller;

import org.apache.seatunnel.app.aspect.UserId;
import org.apache.seatunnel.app.common.Result;
import org.apache.seatunnel.app.domain.request.task.ExecuteReq;
import org.apache.seatunnel.app.domain.request.task.InstanceListReq;
import org.apache.seatunnel.app.domain.request.task.InstanceLogRes;
import org.apache.seatunnel.app.domain.request.task.JobListReq;
import org.apache.seatunnel.app.domain.request.task.RecycleScriptReq;
import org.apache.seatunnel.app.domain.response.PageInfo;
import org.apache.seatunnel.app.domain.response.task.InstanceSimpleInfoRes;
import org.apache.seatunnel.app.domain.response.task.JobSimpleInfoRes;
import org.apache.seatunnel.app.service.ITaskService;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import springfox.documentation.annotations.ApiIgnore;

import javax.annotation.Resource;
import javax.validation.constraints.NotNull;

@RequestMapping("/api/v1/task")
@RestController
public class TaskController {

    @Resource
    private ITaskService iTaskService;

    @PatchMapping("/{scriptId}/recycle")
    @ApiOperation(value = "recycle script", httpMethod = "PATCH")
    Result<Void> recycle(@ApiParam(value = "script id", required = true) @PathVariable(value = "scriptId") Integer scriptId,
                         @ApiIgnore @UserId Integer operatorId) {
        final RecycleScriptReq req = new RecycleScriptReq();
        req.setScriptId(scriptId);
        req.setOperatorId(operatorId);

        iTaskService.recycleScriptFromScheduler(req);
        return Result.success();
    }

    @GetMapping("/job")
    @ApiOperation(value = "list job", httpMethod = "GET")
    Result<PageInfo<JobSimpleInfoRes>> listJob(@ApiParam(value = "job name") @RequestParam(required = false) String name,
                                               @ApiParam(value = "page num", required = true) @RequestParam Integer pageNo,
                                               @ApiParam(value = "page size", required = true) @RequestParam Integer pageSize) {
        final JobListReq req = new JobListReq();
        req.setName(name);
        req.setPageNo(pageNo);
        req.setPageSize(pageSize);

        return Result.success(iTaskService.listJob(req));
    }

    @GetMapping("/instance")
    @ApiOperation(value = "list instance", httpMethod = "GET")
    Result<PageInfo<InstanceSimpleInfoRes>> listInstance(@ApiParam(value = "job name") @RequestParam(required = false) String name,
                                                         @ApiParam(value = "page num", required = true) @RequestParam Integer pageNo,
                                                         @ApiParam(value = "page size", required = true) @RequestParam Integer pageSize) {
        final InstanceListReq req = new InstanceListReq();
        req.setName(name);
        req.setPageNo(pageNo);
        req.setPageSize(pageSize);

        return Result.success(iTaskService.listInstance(req));
    }

    @PostMapping("/{scriptId}/execute")
    @ApiOperation(value = "execute script temporary", httpMethod = "POST")
    Result<InstanceSimpleInfoRes> tmpExecute(@ApiParam(value = "script id", required = true) @PathVariable(value = "scriptId") Integer scriptId,
                                             @RequestBody @NotNull ExecuteReq req,
                                             @ApiIgnore @UserId Integer operatorId) {
        req.setScriptId(scriptId);
        req.setOperatorId(operatorId);

        return Result.success(iTaskService.tmpExecute(req));
    }

    @GetMapping("/{taskInstanceId}")
    @ApiOperation(value = "query instance log", httpMethod = "GET")
    Result<InstanceLogRes> queryInstanceLog(@ApiParam(value = "task instance id", required = true) @PathVariable(value = "taskInstanceId") Long taskInstanceId) {
        return Result.success(iTaskService.queryInstanceLog(taskInstanceId));
    }

    @PatchMapping("/{taskInstanceId}")
    @ApiOperation(value = "kill running instance", httpMethod = "POST")
    Result<Void> kill(@ApiParam(value = "task instance id", required = true) @PathVariable(value = "taskInstanceId") Long taskInstanceId) {
        iTaskService.kill(taskInstanceId);
        return Result.success();
    }
}
