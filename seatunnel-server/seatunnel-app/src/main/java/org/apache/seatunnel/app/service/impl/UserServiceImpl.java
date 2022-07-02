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

package org.apache.seatunnel.app.service.impl;

import org.apache.seatunnel.app.dal.dao.IUserDao;
import org.apache.seatunnel.app.dal.entity.User;
import org.apache.seatunnel.app.domain.dto.user.ListUserDto;
import org.apache.seatunnel.app.domain.dto.user.UpdateUserDto;
import org.apache.seatunnel.app.domain.request.user.AddUserReq;
import org.apache.seatunnel.app.domain.request.user.UpdateUserReq;
import org.apache.seatunnel.app.domain.request.user.UserListReq;
import org.apache.seatunnel.app.domain.response.user.UserSimpleInfoRes;
import org.apache.seatunnel.app.service.IUserService;
import org.apache.seatunnel.app.util.PasswordUtils;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

import java.util.List;
import java.util.stream.Collectors;

@Component
public class UserServiceImpl implements IUserService {
    @Resource
    private IUserDao userDaoImpl;

    @Value("${user.default.passwordSalt:seatunnel}")
    private String defaultSalt;

    @Override
    public int add(AddUserReq addReq) {
        // 1. check duplicate user first
        userDaoImpl.checkUserExists(addReq.getUsername());

        // 2. add a new user.
        final UpdateUserDto dto = UpdateUserDto.builder()
                .id(null)
                .username(addReq.getUsername())
                // encryption user's password
                .password(PasswordUtils.encryptWithSalt(defaultSalt, addReq.getPassword()))
                .status(addReq.getStatus())
                .type(addReq.getType())
                .build();

        return userDaoImpl.add(dto);
    }

    @Override
    public void update(UpdateUserReq updateReq) {
        final UpdateUserDto dto = UpdateUserDto.builder()
                .id(updateReq.getId())
                .username(updateReq.getUsername())
                // encryption user's password
                .password(PasswordUtils.encryptWithSalt(defaultSalt, updateReq.getPassword()))
                .status(updateReq.getStatus())
                .type(updateReq.getType())
                .build();

        userDaoImpl.update(dto);
    }

    @Override
    public void delete(int id) {
        userDaoImpl.delete(id);
    }

    @Override
    public List<UserSimpleInfoRes> list(UserListReq userListReq) {

        final ListUserDto dto = ListUserDto.builder()
                .name(userListReq.getName())
                .build();

        List<User> userList = userDaoImpl.list(dto, userListReq.getPageNo(), userListReq.getPageSize());
        return userList.stream().map(this::translate).collect(Collectors.toList());
    }

    @Override
    public void enable(int id) {
        userDaoImpl.enable(id);
    }

    @Override
    public void disable(int id) {
        userDaoImpl.disable(id);
    }

    private UserSimpleInfoRes translate(User user) {
        final UserSimpleInfoRes info = new UserSimpleInfoRes();
        info.setId(user.getId());
        info.setStatus(user.getStatus());
        info.setType(user.getType());
        info.setCreateTime(user.getCreateTime());
        info.setUpdateTime(user.getUpdateTime());
        info.setName(user.getUsername());
        return info;
    }
}
