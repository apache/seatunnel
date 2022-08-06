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

import org.apache.seatunnel.app.common.Result;
import org.apache.seatunnel.app.domain.request.task.ExecuteReq;
import org.apache.seatunnel.app.domain.request.task.InstanceListReq;
import org.apache.seatunnel.app.domain.request.task.JobListReq;
import org.apache.seatunnel.app.domain.request.task.RecycleScriptReq;
import org.apache.seatunnel.app.domain.response.task.InstanceSimpleInfoRes;
import org.apache.seatunnel.app.domain.response.task.JobSimpleInfoRes;
import org.apache.seatunnel.app.service.ITaskService;

import io.swagger.annotations.ApiOperation;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import javax.validation.constraints.NotNull;

import java.util.List;

@RequestMapping("/api/v1/task")
@RestController
public class TaskController {

    @Resource
    private ITaskService iTaskService;

    @PutMapping("/recycle")
    @ApiOperation(value = "recycle script", httpMethod = "PUT")
    Result<Void> recycle(@RequestBody @NotNull RecycleScriptReq req) {
        iTaskService.recycleScriptFromScheduler(req);
        return Result.success();
    }

    @GetMapping("/listJob")
    @ApiOperation(value = "list job", httpMethod = "GET")
    Result<List<JobSimpleInfoRes>> listJob(@RequestBody @NotNull JobListReq req) {
        return Result.success(iTaskService.listJob(req));
    }

    @GetMapping("/listInstance")
    @ApiOperation(value = "list instance", httpMethod = "GET")
    Result<List<InstanceSimpleInfoRes>> listInstance(@RequestBody @NotNull InstanceListReq req) {
        return Result.success(iTaskService.listInstance(req));
    }

    @PostMapping("/tmpExecute")
    @ApiOperation(value = "execute script temporary", httpMethod = "GET")
    Result<InstanceSimpleInfoRes> tmpExecute(@RequestBody @NotNull ExecuteReq req) {
        return Result.success(iTaskService.tmpExecute(req));
    }
}
