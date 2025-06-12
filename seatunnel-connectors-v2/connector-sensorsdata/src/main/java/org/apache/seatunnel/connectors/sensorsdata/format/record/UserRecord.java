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

import com.sensorsdata.analytics.javasdk.SensorsConst;
import com.sensorsdata.analytics.javasdk.bean.schema.UserSchema;
import lombok.Getter;

import java.util.Map;

import static com.sensorsdata.analytics.javasdk.SensorsConst.PROFILE_SET_ACTION_TYPE;

public class UserRecord extends UserRecordBase {

    @Getter private final UserSchema userSchema;

    public UserRecord(UserSchema userSchema) {
        this(userSchema, PROFILE_SET_ACTION_TYPE);
    }

    public UserRecord(UserSchema userSchema, String actionType) {
        super(
                userSchema.getTrackId(),
                userSchema.getDistinctId(),
                userSchema.getIdentityMap(),
                userSchema.getPropertyMap(),
                actionType,
                SensorsConst.USER_SCHEMA);
        this.userSchema = userSchema;
    }

    protected Map<String, Object> toMap() {
        Map<String, Object> data = super.toMapWithOutProperties();
        checkAndSetIdentity(data);
        data.put(SensorsDataJsonKeys.PROPERTIES, userSchema.getPropertyMap());
        return data;
    }
}
