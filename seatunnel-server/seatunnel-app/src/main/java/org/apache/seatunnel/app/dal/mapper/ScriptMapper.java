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

package org.apache.seatunnel.app.dal.mapper;

import org.apache.seatunnel.app.dal.entity.Script;

import org.apache.ibatis.annotations.Param;

import java.util.List;

public interface ScriptMapper {
    Script selectByPrimaryKey(Integer id);

    void insert(Script script);

    void updateContentByPrimaryKey(@Param("id") int id, @Param("content") String content, @Param("contentMd5") String contentMd5, @Param("menderId") int menderId);

    void updateStatus(@Param("id") int id, @Param("code") byte code);

    List<Script> selectBySelectiveAndPage(@Param("script") Script script, @Param("start") int start, @Param("offset") int offset);

    Script selectByNameAndCreatorAndStatusNotEq(@Param("name") String name, @Param("creatorId") int creatorId, @Param("status") byte status);

    int countBySelectiveAndPage(@Param("script") Script script);
}
