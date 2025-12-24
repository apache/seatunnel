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
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.sensorsdata.format.exception.SensorsDataErrorCode;
import org.apache.seatunnel.connectors.sensorsdata.format.exception.SensorsDataException;

import com.sensorsdata.analytics.javasdk.SensorsConst;
import com.sensorsdata.analytics.javasdk.bean.schema.DetailSchema;
import com.sensorsdata.analytics.javasdk.bean.schema.UserEventSchema;
import com.sensorsdata.analytics.javasdk.bean.schema.UserSchema;
import com.sensorsdata.analytics.javasdk.exceptions.InvalidArgumentException;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static com.sensorsdata.analytics.javasdk.SensorsConst.TRACK_ACTION_TYPE;

public abstract class UserRecordBase implements SensorsDataRecord {

    protected static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private Integer trackId;

    private String distinctId;

    private Map<String, Object> identities;

    private String type;

    private Date time;

    /** property name and value */
    private Map<String, Object> properties;

    /** project name of sensorsdata */
    private String project;

    private String token;

    private boolean timeFree = false;

    private String schema;

    public String getType() {
        return type;
    }

    protected UserRecordBase(
            Integer trackId,
            String distinctId,
            Map<String, Object> identities,
            Map<String, Object> properties,
            String type,
            String schema) {
        initBasicFields(distinctId, identities, type, properties, trackId, schema);
    }

    protected void initBasicFields(
            String distinctId,
            Map<String, Object> identities,
            String type,
            Map<String, Object> properties,
            Integer trackId,
            String schema) {
        this.trackId = trackId;
        this.distinctId = distinctId;
        this.identities = identities;
        this.type = type;
        this.schema = schema;
        this.time =
                properties.containsKey(SensorsConst.TIME_SYSTEM_ATTR)
                        ? (Date) properties.get(SensorsConst.TIME_SYSTEM_ATTR)
                        : new Date();
        this.properties = properties;
        this.project =
                properties.get(SensorsConst.PROJECT_SYSTEM_ATTR) == null
                        ? null
                        : String.valueOf(properties.get(SensorsConst.PROJECT_SYSTEM_ATTR));
        this.token =
                properties.get(SensorsConst.TOKEN_SYSTEM_ATTR) == null
                        ? null
                        : String.valueOf(properties.get(SensorsConst.TOKEN_SYSTEM_ATTR));
        this.timeFree =
                properties.containsKey(SensorsConst.TIME_FREE_ATTR)
                        && Boolean.parseBoolean(
                                properties.get(SensorsConst.TIME_FREE_ATTR).toString());
    }

    protected Map<String, Object> toMapWithOutProperties() {
        Map<String, Object> data = new HashMap<>();
        data.put(SensorsDataJsonKeys.TRACK_ID, trackId);
        data.put(SensorsDataJsonKeys.VERSION, SensorsConst.PROTOCOL_VERSION);
        data.put(SensorsDataJsonKeys.TYPE, type);
        data.put(SensorsDataJsonKeys.SCHEMA, schema);
        data.put(SensorsDataJsonKeys.LIB, SensorsDataLibInfo.LIB_INFO);
        data.put(SensorsDataJsonKeys.TIME, time.getTime());
        if (StringUtils.isNotEmpty(project)) {
            data.put(SensorsDataJsonKeys.PROJECT, project);
        }
        if (StringUtils.isNotEmpty(token)) {
            data.put(SensorsDataJsonKeys.TOKEN, token);
        }

        return data;
    }

    protected abstract Map<String, Object> toMap();

    @Override
    public String toJsonString() {
        try {
            return OBJECT_MAPPER.writeValueAsString(this.toMap());
        } catch (JsonProcessingException e) {
            return null;
        }
    }

    protected void addTimeFree(Map<String, Object> data) {
        if (!timeFree) {
            return;
        }
        if (StringUtils.equals(type, TRACK_ACTION_TYPE)) {
            data.put(SensorsDataJsonKeys.TIME_FREE, true);
        }
    }

    protected void checkAndSetIdentity(Map<String, Object> data) {
        if (null != identities && !identities.isEmpty()) {
            data.put(SensorsDataJsonKeys.IDENTITIES, identities);
        }
        if (StringUtils.isNotEmpty(distinctId)) {
            data.put(SensorsDataJsonKeys.DISTINCT_ID, distinctId);
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

        public UserEventRecord buildUserEventRecord(SeaTunnelRow row) {
            try {
                return new UserEventRecord(
                        UserEventSchema.init()
                                .setEventName(rowAccessor.getEventName(row))
                                .setDistinctId(rowAccessor.getDistinctId(row))
                                .identityMap(rowAccessor.getIdentities(row))
                                .addProperties(rowAccessor.getProperties(row))
                                .start());
            } catch (InvalidArgumentException e) {
                throw new SensorsDataException(
                        SensorsDataErrorCode.ILLEGAL_ARGUMENT, e.getMessage());
            }
        }

        public UserDetailRecord buildUserDetailRecord(SeaTunnelRow row) {
            try {
                return new UserDetailRecord(
                        DetailSchema.init()
                                .setSchema(rowAccessor.getSchemaRequired())
                                .setDetailId(rowAccessor.getDetailIdRequired(row))
                                .setDistinctId(rowAccessor.getDistinctId(row))
                                .identityMap(rowAccessor.getIdentities(row))
                                .addProperties(rowAccessor.getProperties(row))
                                .start());
            } catch (InvalidArgumentException e) {
                throw new SensorsDataException(
                        SensorsDataErrorCode.ILLEGAL_ARGUMENT, e.getMessage());
            }
        }

        public UserRecord buildUserRecord(SeaTunnelRow row) {
            try {

                return new UserRecord(
                        UserSchema.init()
                                .setDistinctId(rowAccessor.getDistinctId(row))
                                .identityMap(rowAccessor.getUserIdentities(row))
                                .addProperties(rowAccessor.getProperties(row))
                                .start());
            } catch (InvalidArgumentException e) {
                throw new SensorsDataException(
                        SensorsDataErrorCode.ILLEGAL_ARGUMENT, e.getMessage());
            }
        }
    }
}
