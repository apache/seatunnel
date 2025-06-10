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

package org.apache.seatunnel.format.sensorsdata.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import lombok.Getter;
import lombok.ToString;

import java.io.Serializable;
import java.util.List;

@Getter
@ToString
public class SensorsDataConfigBase implements Serializable {
    protected final String entityName;
    protected final String recordType;
    protected final String schema;
    protected final String distinctIdColumn;
    protected final List<TargetColumnConfig> identityFields;
    protected final List<TargetColumnConfig> propertyFields;
    protected final String eventName;
    protected final String timeColumn;
    protected final String detailIdColumn;
    protected final boolean timeFree;
    protected final boolean skipErrorRecord;
    protected final String itemIdColumn;
    protected final String itemTypeColumn;
    // 配置 distinctIdColumn 值的获取逻辑
    // 默认情况下 false, 表示 distinctId 只能从 distinctIdColumn 字段获取
    // 如果为 true, 则表示 distinctId 可以从 identityFields 字段获取
    protected final boolean distinctIdByIdentities;
    // 配置 profile unset 的逻辑
    // nullAsProfileUnset, 默认情况下为 false, 如当前逻辑, 不处理 properties 中为 null 的情况
    // nullAsProfileUnset  为 true 时, 在 seatunnel 原本 profile_set 发送的逻辑上, 增加对 properties 为 null的处理,
    // 多发送一条 profile_unset 的数据
    protected final boolean nullAsProfileUnset;

    public SensorsDataConfigBase(ReadonlyConfig config) {
        this.entityName = config.get(SensorsDataOptions.ENTITY_NAME);
        this.recordType = config.get(SensorsDataOptions.RECORD_TYPE);
        this.schema = config.get(SensorsDataOptions.SCHEMA);
        this.distinctIdColumn = config.get(SensorsDataOptions.DISTINCT_ID_COLUMN);
        this.identityFields = config.get(SensorsDataOptions.IDENTITY_FIELDS);
        this.propertyFields = config.get(SensorsDataOptions.PROPERTY_FIELDS);
        this.eventName = config.get(SensorsDataOptions.EVENT_NAME);
        this.timeColumn = config.get(SensorsDataOptions.TIME_COLUMN);
        this.detailIdColumn = config.get(SensorsDataOptions.DETAIL_ID_COLUMN);
        this.timeFree = config.get(SensorsDataOptions.TIME_FREE);
        this.skipErrorRecord = config.get(SensorsDataOptions.SKIP_ERROR_RECORD);
        this.itemIdColumn = config.get(SensorsDataOptions.ITEM_ID_COLUMN);
        this.itemTypeColumn = config.get(SensorsDataOptions.ITEM_TYPE_COLUMN);
        this.distinctIdByIdentities = config.get(SensorsDataOptions.DISTINCT_ID_BY_IDENTITIES);
        this.nullAsProfileUnset = config.get(SensorsDataOptions.NULL_AS_PROFILE_UNSET);
    }
}
