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

package org.apache.seatunnel.connectors.sensorsdata.format.record;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.sensorsdata.format.exception.SensorsDataErrorCode;
import org.apache.seatunnel.connectors.sensorsdata.format.exception.SensorsDataException;

import com.sensorsdata.analytics.javasdk.SensorsConst;
import com.sensorsdata.analytics.javasdk.bean.ItemRecord;
import com.sensorsdata.analytics.javasdk.exceptions.InvalidArgumentException;
import lombok.Getter;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static com.sensorsdata.analytics.javasdk.SensorsConst.ITEM_SET_ACTION_TYPE;
import static org.apache.seatunnel.connectors.sensorsdata.format.record.UserRecordBase.OBJECT_MAPPER;

public class SpecialItemRecord implements SensorsDataRecord {

    @Getter private ItemRecord itemRecord;

    private SpecialItemRecord(ItemRecord itemRecord) {
        this.itemRecord = itemRecord;
    }

    private Map<String, Object> toMap() {
        Map<String, Object> data = new HashMap<>();
        data.put(SensorsDataJsonKeys.TYPE, ITEM_SET_ACTION_TYPE);
        data.put(SensorsDataJsonKeys.LIB, SensorsDataLibInfo.LIB_INFO);

        data.put(SensorsDataJsonKeys.ITEM_ID, itemRecord.getItemId());
        data.put(SensorsDataJsonKeys.ITEM_TYPE, itemRecord.getItemType());

        Date time =
                itemRecord.getPropertyMap().containsKey(SensorsConst.TIME_SYSTEM_ATTR)
                        ? (Date) itemRecord.getPropertyMap().remove(SensorsConst.TIME_SYSTEM_ATTR)
                        : new Date();
        data.put(SensorsDataJsonKeys.TIME, time.getTime());

        String project =
                itemRecord.getPropertyMap().get(SensorsConst.PROJECT_SYSTEM_ATTR) == null
                        ? null
                        : String.valueOf(
                                itemRecord
                                        .getPropertyMap()
                                        .remove(SensorsConst.PROJECT_SYSTEM_ATTR));
        if (StringUtils.isNotEmpty(project)) {
            data.put(SensorsDataJsonKeys.PROJECT, project);
        }

        String token =
                itemRecord.getPropertyMap().get(SensorsConst.TOKEN_SYSTEM_ATTR) == null
                        ? null
                        : String.valueOf(
                                itemRecord.getPropertyMap().remove(SensorsConst.TOKEN_SYSTEM_ATTR));
        if (StringUtils.isNotEmpty(token)) {
            data.put(SensorsDataJsonKeys.TOKEN, token);
        }

        data.put(SensorsDataJsonKeys.PROPERTIES, itemRecord.getPropertyMap());
        return data;
    }

    @Override
    public String toJsonString() {
        try {
            return OBJECT_MAPPER.writeValueAsString(this.toMap());
        } catch (JsonProcessingException e) {
            return null;
        }
    }

    public static Builder newBuilder(RowAccessor rowAccessor) {
        return new Builder(rowAccessor);
    }

    public static class Builder {

        private final RowAccessor rowAccessor;

        private Builder(RowAccessor rowAccessor) {
            this.rowAccessor = rowAccessor;
        }

        public SpecialItemRecord build(SeaTunnelRow row) {
            try {
                return new SpecialItemRecord(
                        ItemRecord.builder()
                                .setItemId(rowAccessor.getItemIdRequired(row))
                                .setItemType(rowAccessor.getItemTypeRequired(row))
                                .addProperties(rowAccessor.getProperties(row))
                                .build());
            } catch (InvalidArgumentException e) {
                throw new SensorsDataException(
                        SensorsDataErrorCode.ILLEGAL_ARGUMENT, e.getMessage());
            }
        }
    }
}
