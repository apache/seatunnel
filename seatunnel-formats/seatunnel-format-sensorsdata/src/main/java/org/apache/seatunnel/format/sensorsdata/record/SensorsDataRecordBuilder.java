package org.apache.seatunnel.format.sensorsdata.record;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.format.sensorsdata.config.SensorsDataConfigBase;
import org.apache.seatunnel.format.sensorsdata.exception.SensorsDataErrorCode;
import org.apache.seatunnel.format.sensorsdata.exception.SensorsDataException;

import lombok.Getter;

public class SensorsDataRecordBuilder {

    // Entity Name
    private static final String USER_ENTITY_NAME = "users";

    private static final String SPECIAL_ITEM_ENTITY_NAME = "items";

    // 用户主表
    private static final String USER_RECORD = "users";

    // 物品主表
    private static final String ITEM_RECORD = "items";

    // 用户/物品 事件表
    private static final String EVENT_RECORD = "events";

    // 用户/物品 明细表
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
                    // 物品相关数据暂不支持
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
