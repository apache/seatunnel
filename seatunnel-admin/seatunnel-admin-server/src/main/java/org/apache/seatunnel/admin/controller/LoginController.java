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


import cn.dev33.satoken.stp.StpUtil;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.apache.seatunnel.admin.common.Result;
import org.apache.seatunnel.admin.service.IStUserService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@Api(tags = "LOGIN_TAG")
@RestController
@RequestMapping("")
public class LoginController {

    private static final Logger logger = LoggerFactory.getLogger(LoginController.class);

    @Autowired
    private IStUserService userService;

    @ApiOperation(value = "login", notes = "LOGIN_NOTES")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "username", value = "USER_NAME", required = true, dataType = "String"),
            @ApiImplicitParam(name = "password", value = "USER_PASSWORD", required = true, dataType = "String")
    })
    @PostMapping(value = "/login")
    public Result login(@RequestParam(value = "username") String username,
                        @RequestParam(value = "password") String password) {
        String token = userService.authenticate(username, password);
        return Result.success(token);
    }

    @ApiOperation(value = "signOut", notes = "SIGNOUT_NOTES")
    @PostMapping(value = "/signOut")
    public Result signOut() {
        StpUtil.logout();
        return Result.success(true);
    }

}
