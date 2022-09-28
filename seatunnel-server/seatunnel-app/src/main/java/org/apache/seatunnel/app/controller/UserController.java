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
import org.apache.seatunnel.app.domain.request.user.AddUserReq;
import org.apache.seatunnel.app.domain.request.user.UpdateUserReq;
import org.apache.seatunnel.app.domain.request.user.UserListReq;
import org.apache.seatunnel.app.domain.request.user.UserLoginReq;
import org.apache.seatunnel.app.domain.response.PageInfo;
import org.apache.seatunnel.app.domain.response.user.AddUserRes;
import org.apache.seatunnel.app.domain.response.user.UserSimpleInfoRes;
import org.apache.seatunnel.app.service.IUserService;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import javax.validation.constraints.NotNull;

@RequestMapping("/api/v1/user")
@RestController
public class UserController {

    @Resource
    private IUserService iUserService;

    @PostMapping
    @ApiOperation(value = "add user", httpMethod = "POST")
    public Result<AddUserRes> add(@RequestBody @NotNull AddUserReq addReq) {
        return Result.success(iUserService.add(addReq));
    }

    @PutMapping("/{userId}")
    @ApiOperation(value = "update user", httpMethod = "PUT")
    public Result<Void> update(@ApiParam(value = "user id", required = true) @PathVariable(value = "userId") Integer userId,
                               @RequestBody @NotNull UpdateUserReq updateReq) {
        updateReq.setUserId(userId);

        iUserService.update(updateReq);
        return Result.success();
    }

    @DeleteMapping("/{userId}")
    @ApiOperation(value = "delete user", httpMethod = "DELETE")
    public Result<Void> delete(@ApiParam(value = "user id", required = true) @PathVariable(value = "userId") Integer userId) {
        iUserService.delete(userId);
        return Result.success();
    }

    @GetMapping
    @ApiOperation(value = "user list", httpMethod = "GET")
    public Result<PageInfo<UserSimpleInfoRes>> list(@ApiParam(value = "job name") @RequestParam(required = false) String name,
                                                    @ApiParam(value = "page num", required = true) @RequestParam Integer pageNo,
                                                    @ApiParam(value = "page size", required = true) @RequestParam Integer pageSize) {
        final UserListReq req = new UserListReq();
        req.setName(name);
        req.setPageNo(pageNo);
        req.setPageSize(pageSize);

        return Result.success(iUserService.list(req));
    }

    @PatchMapping("/{userId}/enable")
    @ApiOperation(value = "enable a user", httpMethod = "PATCH")
    public Result<Void> enable(@ApiParam(value = "user id", required = true) @PathVariable(value = "userId") Integer userId) {
        iUserService.enable(userId);
        return Result.success();
    }

    @PutMapping("/{userId}/disable")
    @ApiOperation(value = "disable a user", httpMethod = "PUT")
    public Result<Void> disable(@ApiParam(value = "user id", required = true) @PathVariable(value = "userId") Integer userId) {
        iUserService.disable(userId);
        return Result.success();
    }

    @PostMapping("/login")
    public Result<UserSimpleInfoRes> login(@RequestBody UserLoginReq req) {
        return Result.success(iUserService.login(req));
    }

    @PatchMapping("/logout")
    public Result<Void> logout() {
        return Result.success();
    }
}
