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
package org.apache.seatunnel.admin.entity;

import com.baomidou.mybatisplus.annotation.*;

import java.io.Serializable;
import java.time.LocalDateTime;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

/**
 * <p>
 * 任务实例表
 * </p>
 *
 * @author quanzhian
 * @since 2022-06-28
 */
@TableName("t_st_task_instance")
@ApiModel(value = "StTaskInstance对象", description = "任务实例表")
public class StTaskInstance implements Serializable {

    private static final long serialVersionUID = 1L;

    @ApiModelProperty("主键ID")
    @TableId(value = "id", type = IdType.AUTO)
    private Long id;

    @ApiModelProperty("任务ID")
    private Long taskId;

    @ApiModelProperty("任务实例名称")
    private String name;

    @ApiModelProperty("任务类型: spark、spark-sql,flink、flink-sql")
    private String type;

    @ApiModelProperty("任务配置内容")
    private String configContent;

    @ApiModelProperty("任务执行命令")
    private String command;

    @ApiModelProperty("主机信息表ID")
    private Long hostId;

    @ApiModelProperty("任务执行状态: 1->启动中、2->运行中、3->已完成、4->失败、5->停止")
    private Integer instanceStatus;

    @ApiModelProperty("创建人ID")
    private Integer creatorId;

    @ApiModelProperty("创建时间")
    @TableField(fill = FieldFill.INSERT)
    private LocalDateTime createTime;

    @ApiModelProperty("速率")
    private Long syncRate;

    @ApiModelProperty("当前同步数量")
    private Long syncCurrentRecords;

    @ApiModelProperty("容量")
    private Long syncVolume;

    @ApiModelProperty("任务开始时间")
    private LocalDateTime startTime;

    @ApiModelProperty("任务结束时间")
    private LocalDateTime endTime;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }
    public Long getTaskId() {
        return taskId;
    }

    public void setTaskId(Long taskId) {
        this.taskId = taskId;
    }
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
    public String getConfigContent() {
        return configContent;
    }

    public void setConfigContent(String configContent) {
        this.configContent = configContent;
    }
    public String getCommand() {
        return command;
    }

    public void setCommand(String command) {
        this.command = command;
    }
    public Long getHostId() {
        return hostId;
    }

    public void setHostId(Long hostId) {
        this.hostId = hostId;
    }
    public Integer getInstanceStatus() {
        return instanceStatus;
    }

    public void setInstanceStatus(Integer instanceStatus) {
        this.instanceStatus = instanceStatus;
    }
    public Integer getCreatorId() {
        return creatorId;
    }

    public void setCreatorId(Integer creatorId) {
        this.creatorId = creatorId;
    }
    public LocalDateTime getCreateTime() {
        return createTime;
    }

    public void setCreateTime(LocalDateTime createTime) {
        this.createTime = createTime;
    }
    public Long getSyncRate() {
        return syncRate;
    }

    public void setSyncRate(Long syncRate) {
        this.syncRate = syncRate;
    }
    public Long getSyncCurrentRecords() {
        return syncCurrentRecords;
    }

    public void setSyncCurrentRecords(Long syncCurrentRecords) {
        this.syncCurrentRecords = syncCurrentRecords;
    }
    public Long getSyncVolume() {
        return syncVolume;
    }

    public void setSyncVolume(Long syncVolume) {
        this.syncVolume = syncVolume;
    }
    public LocalDateTime getStartTime() {
        return startTime;
    }

    public void setStartTime(LocalDateTime startTime) {
        this.startTime = startTime;
    }
    public LocalDateTime getEndTime() {
        return endTime;
    }

    public void setEndTime(LocalDateTime endTime) {
        this.endTime = endTime;
    }

    @Override
    public String toString() {
        return "StTaskInstance{" +
            "id=" + id +
            ", taskId=" + taskId +
            ", name=" + name +
            ", type=" + type +
            ", configContent=" + configContent +
            ", command=" + command +
            ", hostId=" + hostId +
            ", instanceStatus=" + instanceStatus +
            ", creatorId=" + creatorId +
            ", createTime=" + createTime +
            ", syncRate=" + syncRate +
            ", syncCurrentRecords=" + syncCurrentRecords +
            ", syncVolume=" + syncVolume +
            ", startTime=" + startTime +
            ", endTime=" + endTime +
        "}";
    }
}
