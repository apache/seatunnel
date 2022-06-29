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

import com.baomidou.mybatisplus.annotation.FieldFill;
import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;

import java.io.Serializable;
import java.time.LocalDateTime;

@TableName("t_st_task_instance")
public class StTaskInstance implements Serializable {

    @TableId(value = "id", type = IdType.AUTO)
    private Long id;

    private Long taskId;

    private String name;

    private String type;

    private String configContent;

    private String command;

    private Long hostId;

    private Integer instanceStatus;

    private Integer creatorId;

    @TableField(fill = FieldFill.INSERT)
    private LocalDateTime createTime;

    private Long syncRate;

    private Long syncCurrentRecords;

    private Long syncVolume;

    private LocalDateTime startTime;

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
