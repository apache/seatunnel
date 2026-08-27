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

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.sensorsdata.format.config.SensorsDataConfigBase;
import org.apache.seatunnel.connectors.sensorsdata.format.exception.SensorsDataErrorCode;
import org.apache.seatunnel.connectors.sensorsdata.format.exception.SensorsDataException;

import lombok.Getter;

public class SensorsDataRecordBuilder {

    // Entity Name
    private static final String USER_ENTITY_NAME = "users";

    private static final String SPECIAL_ITEM_ENTITY_NAME = "items";

    private static final String USER_RECORD = "users";

    private static final String ITEM_RECORD = "items";

    private static final String EVENT_RECORD = "events";

    private static final String DETAIL_RECORD = "details";

    public static SensorsDataRecordBuilder.Builder newBuilder(
            SensorsDataConfigBase config, RowAccessor rowAccessor) {
        return new SensorsDataRecordBuilder.Builder(config, rowAccessor);
    }

    public static SensorsDataRecordBuilder.Builder newBuilder(
            SensorsDataRecordType recordType, RowAccessor rowAccessor) {
        return new SensorsDataRecordBuilder.Builder(recordType, rowAccessor);
    }

    public static class Builder {

        private final RowAccessor rowAccessor;

        @Getter private final SensorsDataRecordType recordType;

        private UserRecordBase.Builder userRecordBuilder = null;

        private SpecialItemRecord.Builder specialItemRecordBuilder = null;

        private Builder(SensorsDataConfigBase config, RowAccessor rowAccessor) {
            this.rowAccessor = rowAccessor;
            switch (config.getEntityName().toLowerCase()) {
                case USER_ENTITY_NAME:
                    switch (config.getRecordType().toLowerCase()) {
                        case USER_RECORD:
                            this.recordType = SensorsDataRecordType.USER;
                            break;
                        case EVENT_RECORD:
                            this.recordType = SensorsDataRecordType.USER_EVENT;
                            break;
                        case DETAIL_RECORD:
                            this.recordType = SensorsDataRecordType.USER_DETAIL;
                            break;
                        default:
                            throw new SensorsDataException(
                                    SensorsDataErrorCode.UNSUPPORTED_RECORD_TYPE,
                                    "Unsupported : " + config.getRecordType());
                    }
                    this.userRecordBuilder = UserRecordBase.newBuilder(rowAccessor);
                    break;
                case SPECIAL_ITEM_ENTITY_NAME:
                    this.recordType = SensorsDataRecordType.SPECIAL_ITEM;
                    this.specialItemRecordBuilder = SpecialItemRecord.newBuilder(rowAccessor);
                    break;
                default:
                    // not support item record yet.
                    throw new SensorsDataException(
                            SensorsDataErrorCode.UNSUPPORTED_RECORD_TYPE,
                            "Unsupported : " + config.getEntityName());
            }
        }

        private Builder(SensorsDataRecordType recordType, RowAccessor rowAccessor) {
            this.rowAccessor = rowAccessor;
            this.recordType = recordType;
            switch (recordType) {
                case USER:
                case USER_EVENT:
                case USER_DETAIL:
                    this.userRecordBuilder = UserRecordBase.newBuilder(rowAccessor);
                    break;
                case SPECIAL_ITEM:
                    this.specialItemRecordBuilder = SpecialItemRecord.newBuilder(rowAccessor);
                    break;
                default:
                    throw new SensorsDataException(
                            SensorsDataErrorCode.UNSUPPORTED_RECORD_TYPE,
                            "Unsupported Record Type: " + recordType);
            }
        }

        public SensorsDataRecord build(SeaTunnelRow row) {
            switch (recordType) {
                case USER:
                    return this.userRecordBuilder.buildUserRecord(row);
                case USER_EVENT:
                    return this.userRecordBuilder.buildUserEventRecord(row);
                case USER_DETAIL:
                    return this.userRecordBuilder.buildUserDetailRecord(row);
                case SPECIAL_ITEM:
                    return this.specialItemRecordBuilder.build(row);
                default:
                    throw new SensorsDataException(
                            SensorsDataErrorCode.UNSUPPORTED_RECORD_TYPE,
                            "Unsupported Record Type: " + recordType);
            }
        }
    }
}
