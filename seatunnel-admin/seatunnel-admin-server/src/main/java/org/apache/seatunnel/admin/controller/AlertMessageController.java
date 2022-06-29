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
import org.apache.seatunnel.admin.dto.AlertMessagePage;
import org.apache.seatunnel.admin.entity.StAlertMessage;
import org.apache.seatunnel.admin.service.IStAlertMessageService;

import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@Api(tags = "ALERT_MESSAGE_TAG")
@RestController
@RequestMapping("/alert/message")
public class AlertMessageController {

    private static final Logger LOGGER = LoggerFactory.getLogger(AlertMessageController.class);

    @Autowired
    private IStAlertMessageService alertMessageService;

    @ApiOperation(value = "queryAlertMessage", notes = "QUERY_ALERT_MESSAGE_NOTES")
    @GetMapping(value = "/list")
    public Result<List<StAlertMessage>> queryPageList(AlertMessagePage messagePage) {
        QueryWrapper<StAlertMessage> queryWrapper = new QueryWrapper<>();
        queryWrapper.eq(StrUtil.isNotBlank(messagePage.getName()), "name", messagePage.getName());
        Page<StAlertMessage> page = new Page<>(messagePage.getPageNo(), messagePage.getPageSize());
        alertMessageService.page(page, queryWrapper);
        return Result.success(page.getRecords());
    }

}
