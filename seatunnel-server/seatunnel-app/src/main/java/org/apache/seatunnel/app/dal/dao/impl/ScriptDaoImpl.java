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

import static org.apache.seatunnel.server.common.SeatunnelErrorEnum.SCRIPT_ALREADY_EXIST;
import static com.google.common.base.Preconditions.checkState;

import org.apache.seatunnel.app.common.ScriptStatusEnum;
import org.apache.seatunnel.app.dal.dao.IScriptDao;
import org.apache.seatunnel.app.dal.entity.Script;
import org.apache.seatunnel.app.dal.mapper.ScriptMapper;
import org.apache.seatunnel.app.domain.dto.script.AddEmptyScriptDto;
import org.apache.seatunnel.app.domain.dto.script.CheckScriptDuplicateDto;
import org.apache.seatunnel.app.domain.dto.script.ListScriptsDto;
import org.apache.seatunnel.app.domain.dto.script.UpdateScriptContentDto;
import org.apache.seatunnel.server.common.PageData;

import org.springframework.stereotype.Repository;

import javax.annotation.Resource;

import java.util.List;
import java.util.Objects;

@Repository
public class ScriptDaoImpl implements IScriptDao {

    @Resource
    private ScriptMapper scriptMapper;

    @Override
    public void checkScriptDuplicate(CheckScriptDuplicateDto dto) {
        final Script script = scriptMapper.selectByNameAndCreatorAndStatusNotEq(dto.getName(), dto.getCreatorId(), (byte) ScriptStatusEnum.DELETED.getCode());
        checkState(Objects.isNull(script), String.format(SCRIPT_ALREADY_EXIST.getTemplate(), dto.getName()));
    }

    @Override
    public int addEmptyScript(AddEmptyScriptDto dto) {
        final Script script = new Script();
        script.setName(dto.getName());
        script.setType(dto.getType());
        script.setStatus(dto.getStatus());
        script.setCreatorId(dto.getCreatorId());
        script.setMenderId(dto.getMenderId());
        scriptMapper.insert(script);
        return script.getId();
    }

    @Override
    public Script getScript(Integer id) {
        return scriptMapper.selectByPrimaryKey(id);
    }

    @Override
    public void updateScriptContent(UpdateScriptContentDto dto) {
        scriptMapper.updateContentByPrimaryKey(dto.getId(), dto.getContent(), dto.getContentMd5(), dto.getMenderId());
    }

    @Override
    public void deleteScript(int id) {
        scriptMapper.updateStatus(id, (byte) ScriptStatusEnum.DELETED.getCode());
    }

    @Override
    public PageData<Script> list(ListScriptsDto dto, Integer pageNo, Integer pageSize) {
        final Script script = new Script();
        script.setName(dto.getName());
        script.setStatus(dto.getStatus());

        final List<Script> scripts = scriptMapper.selectBySelectiveAndPage(script, pageNo * pageSize, pageSize);
        int count = scriptMapper.countBySelectiveAndPage(script);

        return new PageData<Script>(count, scripts);
    }
}
