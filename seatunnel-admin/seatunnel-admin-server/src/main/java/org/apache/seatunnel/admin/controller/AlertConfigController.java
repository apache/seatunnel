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

package org.apache.seatunnel.admin.controller;

import org.apache.seatunnel.admin.common.Result;
import org.apache.seatunnel.admin.dto.AlertConfigPage;
import org.apache.seatunnel.admin.entity.StAlertConfig;
import org.apache.seatunnel.admin.service.IStAlertConfigService;
import org.apache.seatunnel.admin.utils.StringUtils;

import cn.dev33.satoken.annotation.SaCheckLogin;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@Api(tags = "ALERT_CONFIG_TAG")
@RestController
@RequestMapping("/alert/config")
@SaCheckLogin
public class AlertConfigController {

    private static final Logger LOGGER = LoggerFactory.getLogger(AlertConfigController.class);

    @Autowired
    private IStAlertConfigService alertConfigService;

    @ApiOperation(value = "queryAlertConfig", notes = "QUERY_ALERT_CONFIG_NOTES")
    @GetMapping(value = "/list")
    public Result<List<StAlertConfig>> queryPageList(AlertConfigPage alertConfigPage) {
        QueryWrapper<StAlertConfig> queryWrapper = new QueryWrapper<>();
        queryWrapper.lambda()
                .eq(StringUtils.isNotBlank(alertConfigPage.getName()), StAlertConfig::getName, alertConfigPage.getName())
                .eq(StringUtils.isNotBlank(alertConfigPage.getType()), StAlertConfig::getType, alertConfigPage.getType())
                .eq(alertConfigPage.getStatus() != null && alertConfigPage.getStatus() > 0, StAlertConfig::getStatus, alertConfigPage.getStatus())
                .eq(StringUtils.isNotBlank(alertConfigPage.getConfigContent()), StAlertConfig::getConfigContent, alertConfigPage.getConfigContent());
        Page<StAlertConfig> page = new Page<>(alertConfigPage.getPageNo(), alertConfigPage.getPageSize());
        alertConfigService.page(page, queryWrapper);
        return Result.success(page.getRecords());
    }

    @ApiOperation(value = "createAlertConfig", notes = "CREATE_ALERT_CONFIG_NOTES")
    @PostMapping(value = "/create")
    public Result createData(@RequestBody StAlertConfig stAlertConfig) {
        alertConfigService.save(stAlertConfig);
        return Result.success(stAlertConfig.getId());
    }

    @ApiOperation(value = "updateAlertConfig", notes = "UPDATE_ALERT_CONFIG_NOTES")
    @PostMapping(value = "/update")
    public Result updateData(@RequestBody StAlertConfig stAlertConfig) {
        boolean val = alertConfigService.updateById(stAlertConfig);
        return Result.success(val);
    }

    @ApiOperation(value = "delAlertConfigById", notes = "DELETE_ALERT_CONFIG_BY_ID_NOTES")
    @ApiImplicitParams({ @ApiImplicitParam(name = "id", value = "ALERT_CONFIG_ID", dataType = "Long", example = "100") })
    @PostMapping(value = "/delete")
    public Result delById(@RequestParam(value = "id") Long id) {
        boolean val = alertConfigService.removeById(id);
        return Result.success(val);
    }
}
