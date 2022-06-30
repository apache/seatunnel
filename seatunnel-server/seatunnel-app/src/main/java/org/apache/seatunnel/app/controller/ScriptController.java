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

import org.apache.seatunnel.app.common.JsonResult;
import org.apache.seatunnel.app.domain.request.script.AddEmptyScriptReq;
import org.apache.seatunnel.app.domain.request.script.ScriptListReq;
import org.apache.seatunnel.app.domain.request.script.UpdateScriptContentReq;
import org.apache.seatunnel.app.domain.request.script.UpdateScriptParamReq;
import org.apache.seatunnel.app.domain.response.script.ScriptParamRes;
import org.apache.seatunnel.app.domain.response.script.ScriptSimpleInfoRes;
import org.apache.seatunnel.app.service.IScriptService;

import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import javax.validation.constraints.NotNull;

import java.util.List;

@RequestMapping("/script")
@RestController
public class ScriptController {
    @Resource
    private IScriptService iScriptService;

    @PostMapping("/addEmptyScript")
    @ApiOperation(value = "add an empty script", httpMethod = "POST")
    public JsonResult<Void> addEmptyScript(@RequestBody @NotNull AddEmptyScriptReq addEmptyScriptReq) {
        iScriptService.addEmptyScript(addEmptyScriptReq);
        return JsonResult.success();
    }

    @PostMapping("/updateScriptContent")
    @ApiOperation(value = "update script", httpMethod = "POST")
    public JsonResult<Void> updateScriptContent(@RequestBody @NotNull UpdateScriptContentReq updateScriptContentReq) {
        iScriptService.updateScriptContent(updateScriptContentReq);
        return JsonResult.success();
    }

    @PostMapping("/delete")
    @ApiOperation(value = "delete script", httpMethod = "POST")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "id", value = "script id", dataType = "Integer"),
    })
    public JsonResult<Void> delete(@RequestParam @NotNull Integer id) {
        iScriptService.delete(id);
        return JsonResult.success();
    }

    @PostMapping("/list")
    @ApiOperation(value = "script list", httpMethod = "POST")
    public JsonResult<List<ScriptSimpleInfoRes>> list(@RequestBody @NotNull ScriptListReq scriptListReq) {
        return JsonResult.success(iScriptService.list(scriptListReq));
    }

    @GetMapping("/fetchScriptContent")
    @ApiOperation(value = "fetch script content", httpMethod = "POST")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "id", value = "script id", dataType = "Integer"),
    })
    public JsonResult<String> fetchScriptContent(@RequestParam @NotNull Integer id) {
        return JsonResult.success(iScriptService.fetchScriptContent(id));
    }

    @PostMapping("/updateScriptParam")
    @ApiOperation(value = "update script param", httpMethod = "POST")
    public JsonResult<Void> updateScriptParam(@RequestBody @NotNull UpdateScriptParamReq updateScriptParamReq) {
        iScriptService.updateScriptParam(updateScriptParamReq);
        return JsonResult.success();
    }

    @GetMapping("/fetchScriptParam")
    @ApiOperation(value = "fetch script content", httpMethod = "POST")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "id", value = "script id", dataType = "Integer"),
    })
    public JsonResult<List<ScriptParamRes>> fetchScriptParam(@RequestParam @NotNull Integer id) {
        return JsonResult.success(iScriptService.fetchScriptParam(id));
    }
}
