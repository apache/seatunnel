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

package org.apache.seatunnel.app.dal.dao.impl;

import static org.apache.seatunnel.app.common.SeatunnelErrorEnum.NO_SUCH_USER;
import static org.apache.seatunnel.app.common.SeatunnelErrorEnum.USER_ALREADY_EXISTS;
import static com.google.common.base.Preconditions.checkState;

import org.apache.seatunnel.app.common.UserStatusEnum;
import org.apache.seatunnel.app.dal.dao.IUserDao;
import org.apache.seatunnel.app.dal.entity.User;
import org.apache.seatunnel.app.dal.mapper.UserMapper;
import org.apache.seatunnel.app.domain.dto.script.ListUserDto;
import org.apache.seatunnel.app.domain.dto.script.UpdateUserDto;

import org.springframework.stereotype.Repository;

import javax.annotation.Resource;

import java.util.List;
import java.util.Objects;

@Repository
public class UserDaoImpl implements IUserDao {
    @Resource
    private UserMapper userMapper;

    @Override
    public int add(UpdateUserDto dto) {
        final User user = new User();
        user.setUsername(dto.getUsername());
        user.setPassword(dto.getPassword());
        user.setType((byte) dto.getType());
        user.setStatus((byte) dto.getStatus());

        userMapper.insert(user);
        return user.getId();
    }

    @Override
    public void checkUserExists(String username) {
        User user = userMapper.selectByName(username);
        checkState(Objects.isNull(user), String.format(USER_ALREADY_EXISTS.getTemplate(), username));
    }

    @Override
    public void update(UpdateUserDto dto) {
        final User user = new User();
        user.setUsername(dto.getUsername());
        user.setPassword(dto.getPassword());
        user.setType((byte) dto.getType());
        user.setStatus((byte) dto.getStatus());
        user.setId(dto.getId());

        final int i = userMapper.updateByPrimaryKey(user);
        checkState(i == 1, NO_SUCH_USER.getTemplate());
    }

    @Override
    public void delete(int id) {
        userMapper.deleteByPrimaryKey(id);
    }

    @Override
    public void enable(int id) {
        userMapper.updateStatus(id, (byte) UserStatusEnum.ENABLE.getCode());
    }

    @Override
    public void disable(int id) {
        userMapper.updateStatus(id, (byte) UserStatusEnum.DISABLE.getCode());
    }

    @Override
    public List<User> list(ListUserDto dto, int pageNo, int pageSize) {
        final User user = new User();
        user.setUsername(dto.getName());
        return userMapper.selectBySelectiveAndPage(user, pageNo * pageSize, pageSize);
    }
}
