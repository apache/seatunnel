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

import static org.apache.seatunnel.server.common.SeatunnelErrorEnum.USERNAME_PASSWORD_NO_MATCHED;

import org.apache.seatunnel.app.dal.dao.IUserDao;
import org.apache.seatunnel.app.dal.entity.User;
import org.apache.seatunnel.app.domain.dto.user.ListUserDto;
import org.apache.seatunnel.app.domain.dto.user.UpdateUserDto;
import org.apache.seatunnel.app.domain.request.user.AddUserReq;
import org.apache.seatunnel.app.domain.request.user.UpdateUserReq;
import org.apache.seatunnel.app.domain.request.user.UserListReq;
import org.apache.seatunnel.app.domain.request.user.UserLoginReq;
import org.apache.seatunnel.app.domain.response.PageInfo;
import org.apache.seatunnel.app.domain.response.user.AddUserRes;
import org.apache.seatunnel.app.domain.response.user.UserSimpleInfoRes;
import org.apache.seatunnel.app.service.IRoleService;
import org.apache.seatunnel.app.service.IUserService;
import org.apache.seatunnel.app.utils.PasswordUtils;
import org.apache.seatunnel.server.common.PageData;
import org.apache.seatunnel.server.common.SeatunnelException;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@Component
public class UserServiceImpl implements IUserService {
    @Resource
    private IUserDao userDaoImpl;

    @Resource
    private IRoleService roleServiceImpl;

    @Value("${user.default.passwordSalt:seatunnel}")
    private String defaultSalt;

    @Override
    @Transactional(rollbackFor = Exception.class)
    public AddUserRes add(AddUserReq addReq) {
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

        final int userId = userDaoImpl.add(dto);
        final AddUserRes res = new AddUserRes();
        res.setId(userId);

        // 3. add to role
        roleServiceImpl.addUserToRole(userId, addReq.getType().intValue());
        return res;
    }

    @Override
    public void update(UpdateUserReq updateReq) {
        final UpdateUserDto dto = UpdateUserDto.builder()
                .id(updateReq.getUserId())
                .username(updateReq.getUsername())
                // encryption user's password
                .password(PasswordUtils.encryptWithSalt(defaultSalt, updateReq.getPassword()))
                .status(updateReq.getStatus())
                .type(updateReq.getType())
                .build();

        userDaoImpl.update(dto);
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void delete(int id) {
        userDaoImpl.delete(id);
        roleServiceImpl.deleteByUserId(id);
    }

    @Override
    public PageInfo<UserSimpleInfoRes> list(UserListReq userListReq) {

        final ListUserDto dto = ListUserDto.builder()
                .name(userListReq.getName())
                .build();

        final PageData<User> userPageData = userDaoImpl.list(dto, userListReq.getRealPageNo(), userListReq.getPageSize());

        final List<UserSimpleInfoRes> data = userPageData.getData().stream().map(this::translate).collect(Collectors.toList());
        final PageInfo<UserSimpleInfoRes> pageInfo = new PageInfo<>();
        pageInfo.setPageNo(userListReq.getPageNo());
        pageInfo.setPageSize(userListReq.getPageSize());
        pageInfo.setData(data);
        pageInfo.setTotalCount(userPageData.getTotalCount());

        return pageInfo;
    }

    @Override
    public void enable(int id) {
        userDaoImpl.enable(id);
    }

    @Override
    public void disable(int id) {
        userDaoImpl.disable(id);
    }

    @Override
    public UserSimpleInfoRes login(UserLoginReq req) {

        final String username = req.getUsername();
        final String password = PasswordUtils.encryptWithSalt(defaultSalt, req.getPassword());

        final User user = userDaoImpl.checkPassword(username, password);
        if (Objects.isNull(user)) {
            throw new SeatunnelException(USERNAME_PASSWORD_NO_MATCHED);
        }
        return translate(user);
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
