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

import org.apache.seatunnel.app.common.Result;
import org.apache.seatunnel.app.domain.request.script.AddEmptyScriptReq;
import org.apache.seatunnel.app.domain.request.script.PublishScriptReq;
import org.apache.seatunnel.app.domain.request.script.ScriptListReq;
import org.apache.seatunnel.app.domain.request.script.UpdateScriptContentReq;
import org.apache.seatunnel.app.domain.request.script.UpdateScriptParamReq;
import org.apache.seatunnel.app.domain.response.script.AddEmptyScriptRes;
import org.apache.seatunnel.app.domain.response.script.ScriptParamRes;
import org.apache.seatunnel.app.domain.response.script.ScriptSimpleInfoRes;
import org.apache.seatunnel.app.service.IScriptService;

import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import javax.validation.constraints.NotNull;

import java.util.List;

@RequestMapping("/api/v1/script")
@RestController
public class ScriptController {
    @Resource
    private IScriptService iScriptService;

    @PostMapping("/script")
    @ApiOperation(value = "add an empty script", httpMethod = "POST")
    public Result<AddEmptyScriptRes> addEmptyScript(@RequestBody @NotNull AddEmptyScriptReq addEmptyScriptReq) {
        return Result.success(iScriptService.addEmptyScript(addEmptyScriptReq));
    }

    @PutMapping("/scriptContent")
    @ApiOperation(value = "update script", httpMethod = "PUT")
    public Result<Void> updateScriptContent(@RequestBody @NotNull UpdateScriptContentReq updateScriptContentReq) {
        iScriptService.updateScriptContent(updateScriptContentReq);
        return Result.success();
    }

    @DeleteMapping("/script")
    @ApiOperation(value = "delete script", httpMethod = "DELETE")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "id", value = "script id", dataType = "Integer"),
    })
    public Result<Void> delete(@RequestParam @NotNull Integer id) {
        iScriptService.delete(id);
        return Result.success();
    }

    @PostMapping("/list")
    @ApiOperation(value = "script list", httpMethod = "POST")
    public Result<List<ScriptSimpleInfoRes>> list(@RequestBody @NotNull ScriptListReq scriptListReq) {
        return Result.success(iScriptService.list(scriptListReq));
    }

    @GetMapping("/scriptContent")
    @ApiOperation(value = "fetch script content", httpMethod = "GET")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "id", value = "script id", dataType = "Integer"),
    })
    public Result<String> fetchScriptContent(@RequestParam @NotNull Integer id) {
        return Result.success(iScriptService.fetchScriptContent(id));
    }

    @PutMapping("/scriptParam")
    @ApiOperation(value = "update script param", httpMethod = "PUT")
    public Result<Void> updateScriptParam(@RequestBody @NotNull UpdateScriptParamReq updateScriptParamReq) {
        iScriptService.updateScriptParam(updateScriptParamReq);
        return Result.success();
    }

    @GetMapping("/scriptParam")
    @ApiOperation(value = "fetch script param", httpMethod = "GET")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "id", value = "script id", dataType = "Integer"),
    })
    public Result<List<ScriptParamRes>> fetchScriptParam(@RequestParam @NotNull Integer id) {
        return Result.success(iScriptService.fetchScriptParam(id));
    }

    @PutMapping("/publish")
    @ApiOperation(value = "publish script", httpMethod = "PUT")
    public Result<Void> publish(@RequestBody @NotNull PublishScriptReq req) {
        iScriptService.publishScript(req);
        return Result.success();
    }
}
