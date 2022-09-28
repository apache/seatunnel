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

import org.apache.seatunnel.app.dal.dao.IRoleUserRelationDao;
import org.apache.seatunnel.app.dal.entity.RoleUserRelation;
import org.apache.seatunnel.app.dal.mapper.RoleUserRelationMapper;

import org.springframework.stereotype.Repository;

import javax.annotation.Resource;

@Repository
public class RoleUserRelationDaoImpl implements IRoleUserRelationDao {

    @Resource
    private RoleUserRelationMapper roleUserRelationMapper;

    @Override
    public void add(RoleUserRelation roleUserRelation){
        roleUserRelationMapper.insert(roleUserRelation);
    }

    @Override
    public RoleUserRelation getByUserAndRole(Integer userId, Integer roleId) {
        final RoleUserRelation roleUserRelation = roleUserRelationMapper.selectByUserIdAndRoleId(userId, roleId);
        return roleUserRelation;
    }

    @Override
    public void deleteByUserId(Integer userId) {
        roleUserRelationMapper.deleteByUserId(userId);
    }

}
