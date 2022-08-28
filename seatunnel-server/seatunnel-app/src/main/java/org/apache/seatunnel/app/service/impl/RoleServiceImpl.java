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

import static org.apache.seatunnel.server.common.SeatunnelErrorEnum.NO_SUCH_USER;
import static com.google.common.base.Preconditions.checkState;

import org.apache.seatunnel.app.common.RoleTypeEnum;
import org.apache.seatunnel.app.dal.dao.IRoleDao;
import org.apache.seatunnel.app.dal.dao.IRoleUserRelationDao;
import org.apache.seatunnel.app.dal.dao.IUserDao;
import org.apache.seatunnel.app.dal.entity.Role;
import org.apache.seatunnel.app.dal.entity.RoleUserRelation;
import org.apache.seatunnel.app.dal.entity.User;
import org.apache.seatunnel.app.service.IRoleService;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

import java.util.Objects;

@Service
@Slf4j
public class RoleServiceImpl implements IRoleService {

    @Resource
    private IRoleDao roleDaoImpl;

    @Resource
    private IRoleUserRelationDao roleUserRelationDaoImpl;

    @Resource
    private IUserDao userDaoImpl;

    @Override
    public boolean addUserToRole(Integer userId, Integer type){

        String roleName = type == RoleTypeEnum.ADMIN.getCode() ? RoleTypeEnum.ADMIN.getDescription() : RoleTypeEnum.NORMAL.getDescription();

        final Role role = roleDaoImpl.getByRoleName(roleName);

        final RoleUserRelation build = RoleUserRelation.builder()
                .roleId(role.getId())
                .userId(userId)
                .build();

        roleUserRelationDaoImpl.add(build);
        return true;
    }

    @Override
    public boolean checkUserRole(String username, String roleName){

        final User user = userDaoImpl.getByName(username);

        checkState(!Objects.isNull(user), NO_SUCH_USER.getTemplate());

        final Role role = roleDaoImpl.getByRoleName(roleName);

        final RoleUserRelation byUserAndRole = roleUserRelationDaoImpl.getByUserAndRole(user.getId(), role.getId());

        return !Objects.isNull(byUserAndRole);

    }

    @Override
    public void deleteByUserId(Integer userId) {
        roleUserRelationDaoImpl.deleteByUserId(userId);
    }

}
