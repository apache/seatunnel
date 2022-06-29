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

import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.apache.seatunnel.admin.common.Constants;
import org.apache.seatunnel.admin.common.Result;
import org.apache.seatunnel.admin.entity.StAlertGroup;
import org.apache.seatunnel.admin.service.IStAlertGroupService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@Api(tags = "ALERT_GROUP_TAG")
@RestController
@RequestMapping("/alert/group")
public class AlertGroupController {

    private static final Logger logger = LoggerFactory.getLogger(AlertGroupController.class);

    @Autowired
    private IStAlertGroupService alertGroupService;

    @ApiOperation(value = "queryAlertGroup", notes = "QUERY_ALERT_GROUP_NOTES")
    @GetMapping(value = "/list")
    public Result<List<StAlertGroup>> queryPageList(@RequestParam(name = "search", required = false, defaultValue = "") String search,
                                                    @RequestParam(name = Constants.PAGE_NUMBER, defaultValue = "1") Integer pageNo,
                                                    @RequestParam(name = Constants.PAGE_SIZE, defaultValue = "10") Integer pageSize) {
        QueryWrapper<StAlertGroup> queryWrapper = new QueryWrapper<>();
        queryWrapper.eq(StrUtil.isNotBlank(search), "name", search);
        Page<StAlertGroup> page = new Page<>(pageNo, pageSize);
        alertGroupService.page(page, queryWrapper);
        return Result.success(page.getRecords());
    }

    @ApiOperation(value = "createAlertGroup", notes = "CREATE_ALERT_GROUP_NOTES")
    @PostMapping(value = "/create")
    public Result createData(@RequestBody StAlertGroup alertGroup) {
        alertGroupService.save(alertGroup);
        return Result.success(alertGroup.getId());
    }

    @ApiOperation(value = "updateAlertGroup", notes = "UPDATE_ALERT_GROUP_NOTES")
    @PostMapping(value = "/update")
    public Result updateData(@RequestBody StAlertGroup alertGroup) {
        boolean val = alertGroupService.updateById(alertGroup);
        return Result.success(val);
    }

    @ApiOperation(value = "delAlertGroupById", notes = "DELETE_ALERT_GROUP_BY_ID_NOTES")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "id", value = "ALERT_GROUP_ID", dataType = "Long", example = "100")
    })
    @PostMapping(value = "/delete")
    public Result delById(@RequestParam(value = "id") Long id) {
        boolean val = alertGroupService.removeById(id);
        return Result.success(val);
    }
}
