package org.apache.seatunnel.format.sensorsdata.record;

public enum SensorsDataRecordType {
    // 用户主表数据
    USER,
    // 用户事件表
    USER_EVENT,
    // 用户明细表
    USER_DETAIL,
    // 老架构兼容物品表(仅双主键主表，因为其特殊性，需单独成一个类型)
    SPECIAL_ITEM,
    // 物品主表,暂未实现
    ITEM,
    // 物品事件表,暂未实现
    ITEM_EVENT,
    // 物品明细表,暂未实现
    ITEM_DETAIL
}
