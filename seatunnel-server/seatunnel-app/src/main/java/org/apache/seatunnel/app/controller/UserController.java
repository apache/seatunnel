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
import org.apache.seatunnel.app.domain.response.user.UserSimpleInfoRes;
import org.apache.seatunnel.app.service.IUserService;

import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import javax.validation.constraints.NotNull;

import java.util.List;

@RequestMapping("/api/v1/user")
@RestController
public class UserController {

    @Resource
    private IUserService iUserService;

    @PostMapping("/add")
    @ApiOperation(value = "add user", httpMethod = "POST")
    public Result<Void> add(@RequestBody @NotNull AddUserReq addReq) {
        iUserService.add(addReq);
        return Result.success();
    }

    @PutMapping("/update")
    @ApiOperation(value = "update user", httpMethod = "POST")
    public Result<Void> update(@RequestBody @NotNull UpdateUserReq updateReq) {
        iUserService.update(updateReq);
        return Result.success();
    }

    @DeleteMapping("/delete")
    @ApiOperation(value = "delete user", httpMethod = "POST")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "id", value = "user id", dataType = "Integer"),
    })
    public Result<Void> delete(@RequestParam @NotNull Integer id) {
        iUserService.delete(id);
        return Result.success();
    }

    @PostMapping("/list")
    @ApiOperation(value = "user list", httpMethod = "POST")
    public Result<List<UserSimpleInfoRes>> list(@RequestBody @NotNull UserListReq userListReq) {
        return Result.success(iUserService.list(userListReq));
    }

    @PutMapping("/enable")
    @ApiOperation(value = "enable a user", httpMethod = "POST")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "id", value = "user id", dataType = "Integer"),
    })
    public Result<Void> enable(@RequestParam @NotNull Integer id) {
        iUserService.enable(id);
        return Result.success();
    }

    @PutMapping("/disable")
    @ApiOperation(value = "disable a user", httpMethod = "POST")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "id", value = "user id", dataType = "Integer"),
    })
    public Result<Void> disable(@RequestParam @NotNull Integer id) {
        iUserService.disable(id);
        return Result.success();
    }
}
