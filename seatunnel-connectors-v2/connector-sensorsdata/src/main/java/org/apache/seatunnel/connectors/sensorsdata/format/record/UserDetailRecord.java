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

import com.sensorsdata.analytics.javasdk.bean.schema.DetailSchema;
import lombok.Getter;

import java.util.Map;

import static com.sensorsdata.analytics.javasdk.SensorsConst.DETAIL_SET_ACTION_TYPE;

public class UserDetailRecord extends UserRecordBase {

    @Getter private final DetailSchema userDetailSchema;

    public UserDetailRecord(DetailSchema userDetailSchema) {
        super(
                userDetailSchema.getTrackId(),
                userDetailSchema.getDistinctId(),
                userDetailSchema.getIdentities(),
                userDetailSchema.getProperties(),
                DETAIL_SET_ACTION_TYPE,
                userDetailSchema.getSchema());
        this.userDetailSchema = userDetailSchema;
    }

    protected Map<String, Object> toMap() {
        Map<String, Object> data = super.toMapWithOutProperties();
        data.put(SensorsDataJsonKeys.ID, userDetailSchema.getDetailId());
        Map<String, Object> properties = this.userDetailSchema.getProperties();
        if (userDetailSchema.getItemPair() != null) {
            properties.put(
                    userDetailSchema.getItemPair().getKey(),
                    userDetailSchema.getItemPair().getValue());
        }
        if (!userDetailSchema.getIdentities().isEmpty()) {
            checkAndSetIdentity(properties);
        }
        data.put(SensorsDataJsonKeys.PROPERTIES, properties);
        return data;
    }
}
