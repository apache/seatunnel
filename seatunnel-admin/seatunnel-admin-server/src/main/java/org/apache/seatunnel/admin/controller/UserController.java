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
import org.apache.seatunnel.admin.dto.UserPage;
import org.apache.seatunnel.admin.dto.UserParam;
import org.apache.seatunnel.admin.dto.UserPasswordParam;
import org.apache.seatunnel.admin.entity.StUser;
import org.apache.seatunnel.admin.enums.ResultStatus;
import org.apache.seatunnel.admin.exception.SeatunnelServiceException;
import org.apache.seatunnel.admin.service.IStUserService;

import cn.dev33.satoken.annotation.SaCheckLogin;
import cn.dev33.satoken.stp.StpUtil;
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

@Api(tags = "USERS_TAG")
@RestController
@RequestMapping("/user")
@SaCheckLogin
public class UserController {

    private static final Logger LOGGER = LoggerFactory.getLogger(UserController.class);

    @Autowired
    private IStUserService userService;

    @ApiOperation(value = "queryUser", notes = "QUERY_USER_NOTES")
    @GetMapping(value = "/list")
    public Result<List<StUser>> queryPageList(UserPage userPage) {
        QueryWrapper<StUser> queryWrapper = new QueryWrapper<>();
        queryWrapper
                .eq(StrUtil.isNotBlank(userPage.getUsername()), "username", userPage.getUsername())
                .eq(StrUtil.isNotBlank(userPage.getType()), "type", userPage.getType())
                .eq(userPage.getStatus() != null && userPage.getStatus() > 0, "status", userPage.getStatus());
        Page<StUser> page = new Page<>(userPage.getPageNo(), userPage.getPageSize());
        userService.page(page, queryWrapper);
        return Result.success(page.getRecords());
    }

    @ApiOperation(value = "createUser", notes = "CREATE_USER_NOTES")
    @PostMapping(value = "/create")
    public Result createUser(@RequestBody UserParam userParam) {
        StUser stUser = userService.createUser(userParam.getUsername(), userParam.getPassword(), userParam.getType(), userParam.getEmail());
        return Result.success(stUser.getId());
    }

    @ApiOperation(value = "updateUser", notes = "UPDATE_USER_NOTES")
    @PostMapping(value = "/update")
    public Result updateUser(@RequestBody UserParam userParam) {
        StUser stUser = userService.updateUser(userParam.getId(), userParam.getUsername(), userParam.getType(), userParam.getEmail(), userParam.getStatus());
        if (StrUtil.isNotBlank(userParam.getPassword())) {
            userService.updatePassword(stUser.getId(), userParam.getPassword());
        }
        return Result.success(stUser.getId());
    }

    @ApiOperation(value = "updatePassword", notes = "UPDATE_USER_PASSWORD_NOTES")
    @PostMapping(value = "/updatePassword")
    public Result updateUserPassword(@RequestBody UserPasswordParam userPasswordParam) {
        if (StrUtil.isBlank(userPasswordParam.getPassword())) {
            throw new SeatunnelServiceException(ResultStatus.REQUEST_PARAMS_NOT_VALID_ERROR, "password");
        }
        boolean val = userService.updatePassword(userPasswordParam.getId(), userPasswordParam.getPassword());
        return Result.success(val);
    }

    @ApiOperation(value = "delUserById", notes = "DELETE_USER_BY_ID_NOTES")
    @ApiImplicitParams({ @ApiImplicitParam(name = "id", value = "USER_ID", dataType = "Int", example = "100") })
    @PostMapping(value = "/delete")
    public Result delUserById(@RequestParam(value = "id") int id) {
        return Result.success(userService.removeById(id));
    }

    @ApiOperation(value = "getCurrentUserInfo", notes = "GET_CURRENT_USER_INFO_NOTES")
    @GetMapping(value = "/currentInfo")
    public Result getCurrentUserInfo() {
        int userId = StpUtil.getLoginIdAsInt();
        StUser stUser = userService.getById(userId);
        return Result.success(stUser);
    }

}
