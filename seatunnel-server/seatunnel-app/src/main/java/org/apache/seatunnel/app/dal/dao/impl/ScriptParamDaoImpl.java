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

import org.apache.seatunnel.app.common.ScriptParamStatusEnum;
import org.apache.seatunnel.app.dal.dao.IScriptParamDao;
import org.apache.seatunnel.app.dal.entity.ScriptParam;
import org.apache.seatunnel.app.dal.mapper.ScriptParamMapper;
import org.apache.seatunnel.app.domain.dto.script.UpdateScriptParamDto;

import com.google.common.collect.Lists;
import org.springframework.stereotype.Repository;

import javax.annotation.Resource;

import java.util.List;
import java.util.Map;

@Repository
public class ScriptParamDaoImpl implements IScriptParamDao {
    @Resource
    private ScriptParamMapper scriptParamMapper;

    @Override
    public List<ScriptParam> getParamsByScriptId(int id) {
        return scriptParamMapper.selectByScriptId(id);
    }

    @Override
    public void updateStatusByScriptId(int scriptId, int code) {
        scriptParamMapper.updateStatusByScriptId(scriptId, (byte) code);
    }

    @Override
    public void batchInsert(UpdateScriptParamDto dto) {
        final Map<String, String> keyAndValue = dto.getParams();
        final List<ScriptParam> scriptParams = Lists.newArrayListWithCapacity(keyAndValue.size());
        keyAndValue.forEach((k, v) -> {
            final ScriptParam scriptParam = new ScriptParam();
            scriptParam.setStatus((byte) ScriptParamStatusEnum.NORMAL.getCode());
            scriptParam.setKey(k);
            scriptParam.setValue(v);
            scriptParam.setScriptId(dto.getScriptId());
            scriptParams.add(scriptParam);
        });

        scriptParamMapper.batchInsert(scriptParams);
    }
}
