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

package org.apache.seatunnel.app.controller;

import org.apache.seatunnel.app.aspect.UserId;
import org.apache.seatunnel.app.common.Result;
import org.apache.seatunnel.app.domain.request.script.AddEmptyScriptReq;
import org.apache.seatunnel.app.domain.request.script.PublishScriptReq;
import org.apache.seatunnel.app.domain.request.script.ScriptListReq;
import org.apache.seatunnel.app.domain.request.script.UpdateScriptContentReq;
import org.apache.seatunnel.app.domain.request.script.UpdateScriptParamReq;
import org.apache.seatunnel.app.domain.response.PageInfo;
import org.apache.seatunnel.app.domain.response.script.AddEmptyScriptRes;
import org.apache.seatunnel.app.domain.response.script.ScriptParamRes;
import org.apache.seatunnel.app.domain.response.script.ScriptSimpleInfoRes;
import org.apache.seatunnel.app.service.IScriptService;

import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import springfox.documentation.annotations.ApiIgnore;

import javax.annotation.Resource;
import javax.validation.constraints.NotNull;

import java.util.List;

@RequestMapping("/api/v1/script")
@RestController
public class ScriptController {
    @Resource
    private IScriptService iScriptService;

    @PostMapping
    @ApiOperation(value = "add an empty script", httpMethod = "POST")
    public Result<AddEmptyScriptRes> addEmptyScript(@RequestBody @NotNull AddEmptyScriptReq addEmptyScriptReq,
                                                    @ApiIgnore @UserId Integer operatorId) {
        addEmptyScriptReq.setCreatorId(operatorId);
        return Result.success(iScriptService.addEmptyScript(addEmptyScriptReq));
    }

    @PutMapping("/{scriptId}/content")
    @ApiOperation(value = "update script", httpMethod = "PUT")
    public Result<Void> updateScriptContent(@ApiParam(value = "script id", required = true) @PathVariable(value = "scriptId") Integer scriptId,
                                            @RequestBody @NotNull String content,
                                            @ApiIgnore @UserId Integer operatorId) {
        final UpdateScriptContentReq req = new UpdateScriptContentReq();
        req.setScriptId(scriptId);
        req.setContent(content);
        req.setMenderId(operatorId);

        iScriptService.updateScriptContent(req);
        return Result.success();
    }

    @DeleteMapping("/{scriptId}")
    @ApiOperation(value = "delete script", httpMethod = "DELETE")
    public Result<Void> delete(@ApiParam(value = "script id", required = true) @PathVariable(value = "scriptId") Integer scriptId) {
        iScriptService.delete(scriptId);
        return Result.success();
    }

    @GetMapping
    @ApiOperation(value = "script list", httpMethod = "GET")
    public Result<PageInfo<ScriptSimpleInfoRes>> list(@ApiParam(value = "script name") @RequestParam(required = false) String name,
                                                      @ApiParam(value = "script status") @RequestParam(required = false) Byte status,
                                                      @ApiParam(value = "page num", required = true) @RequestParam Integer pageNo,
                                                      @ApiParam(value = "page size", required = true) @RequestParam Integer pageSize) {

        final ScriptListReq req = new ScriptListReq();
        req.setName(name);
        req.setStatus(status);
        req.setPageNo(pageNo);
        req.setPageSize(pageSize);

        return Result.success(iScriptService.list(req));
    }

    @GetMapping("/{scriptId}/content")
    @ApiOperation(value = "fetch script content", httpMethod = "GET")
    public Result<String> fetchScriptContent(@ApiParam(value = "script id", required = true) @PathVariable(value = "scriptId") Integer scriptId) {
        return Result.success(iScriptService.fetchScriptContent(scriptId));
    }

    @PutMapping("/{scriptId}/param")
    @ApiOperation(value = "update script param", httpMethod = "PUT")
    public Result<Void> updateScriptParam(@ApiParam(value = "script id", required = true) @PathVariable(value = "scriptId") Integer scriptId,
                                          @RequestBody @NotNull UpdateScriptParamReq updateScriptParamReq) {
        updateScriptParamReq.setScriptId(scriptId);
        iScriptService.updateScriptParam(updateScriptParamReq);
        return Result.success();
    }

    @GetMapping("/{scriptId}/param")
    @ApiOperation(value = "fetch script param", httpMethod = "GET")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "id", value = "script id", dataType = "Integer"),
    })
    public Result<List<ScriptParamRes>> fetchScriptParam(@ApiParam(value = "script id", required = true) @PathVariable(value = "scriptId") Integer scriptId) {
        return Result.success(iScriptService.fetchScriptParam(scriptId));
    }

    @PatchMapping("/{scriptId}/publish")
    @ApiOperation(value = "publish script", httpMethod = "PATCH")
    public Result<Void> publish(@ApiParam(value = "script id", required = true) @PathVariable(value = "scriptId") Integer scriptId,
                                @ApiIgnore @UserId Integer operatorId) {

        final PublishScriptReq req = new PublishScriptReq();
        req.setScriptId(scriptId);
        req.setOperatorId(operatorId);

        iScriptService.publishScript(req);
        return Result.success();
    }
}
