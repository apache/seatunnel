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

package org.apache.seatunnel.app.dal.dao;

import org.apache.seatunnel.app.dal.entity.User;
import org.apache.seatunnel.app.dal.entity.UserLoginLog;
import org.apache.seatunnel.app.domain.dto.user.ListUserDto;
import org.apache.seatunnel.app.domain.dto.user.UpdateUserDto;
import org.apache.seatunnel.app.domain.dto.user.UserLoginLogDto;
import org.apache.seatunnel.server.common.PageData;

public interface IUserDao {
    int add(UpdateUserDto dto);

    void checkUserExists(String username);

    void update(UpdateUserDto dto);

    void delete(int id);

    void enable(int id);

    void disable(int id);

    PageData<User> list(ListUserDto dto, int pageNo, int pageSize);

    User getById(int operatorId);

    User getByName(String user);

    User checkPassword(String username, String password);

    long insertLoginLog(UserLoginLogDto dto);

    void disableToken(int userId);

    UserLoginLog getLastLoginLog(Integer userId);
}
