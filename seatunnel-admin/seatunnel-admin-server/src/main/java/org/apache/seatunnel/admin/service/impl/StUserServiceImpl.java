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

package org.apache.seatunnel.admin.service.impl;

import org.apache.seatunnel.admin.common.Constants;
import org.apache.seatunnel.admin.entity.StUser;
import org.apache.seatunnel.admin.enums.ResultStatus;
import org.apache.seatunnel.admin.exception.SeatunnelServiceException;
import org.apache.seatunnel.admin.mapper.StUserMapper;
import org.apache.seatunnel.admin.service.IStUserService;
import org.apache.seatunnel.admin.utils.PasswordUtil;
import org.apache.seatunnel.admin.utils.RandomUtil;

import cn.dev33.satoken.stp.StpUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.stereotype.Service;

@Service
public class StUserServiceImpl extends ServiceImpl<StUserMapper, StUser> implements IStUserService {

    @Override
    public StUser createUser(String username, String password, Integer type, String email) {
        StUser stUser = new StUser();
        int userId = StpUtil.getLoginIdAsInt();
        String salt = RandomUtil.generateSalt(Constants.RANDOM_PLACE_NUM);
        String encryptPwd = PasswordUtil.encrypt(password, salt);
        stUser.setCreatorId(userId);
        stUser.setSalt(salt);
        stUser.setUsername(username);
        stUser.setPassword(encryptPwd);
        stUser.setType(type);
        stUser.setEmail(email);
        stUser.setStatus(Constants.DEFAULT_STATUS_VALUE);
        this.save(stUser);
        return stUser;
    }

    @Override
    public StUser updateUser(Integer id, String username, Integer type, String email, Integer status) {
        StUser stUser = this.getById(id);
        int userId = StpUtil.getLoginIdAsInt();
        stUser.setUsername(username);
        stUser.setType(type);
        stUser.setEmail(email);
        stUser.setStatus(status);
        stUser.setMenderId(userId);
        this.updateById(stUser);
        return stUser;
    }

    @Override
    public Boolean updatePassword(Integer id, String password) {
        StUser stUser = this.getById(id);
        String salt = RandomUtil.generateSalt(Constants.RANDOM_PLACE_NUM);
        String encryptPwd = PasswordUtil.encrypt(password, salt);
        stUser.setSalt(salt);
        stUser.setPassword(encryptPwd);
        return this.updateById(stUser);
    }

    @Override
    public StUser selectByUsername(String username) {
        QueryWrapper queryWrapper = new QueryWrapper();
        queryWrapper.eq("username", username);
        return this.getOne(queryWrapper);
    }

    @Override
    public String authenticate(String username, String password) {
        StUser stUser = this.selectByUsername(username);
        String encryptPwd = PasswordUtil.encrypt(password, stUser.getSalt());
        if (stUser.getPassword().equalsIgnoreCase(encryptPwd)) {
            StpUtil.login(stUser.getId());
            String token = StpUtil.getTokenValueByLoginId(stUser.getId());
            return token;
        }
        throw new SeatunnelServiceException(ResultStatus.USER_NAME_PASSWD_ERROR);
    }
}
