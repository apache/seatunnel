package org.apache.seatunnel.format.sensorsdata.config;

import org.apache.seatunnel.api.configuration.util.OptionRule;

/**
 * TODO
 *
 * @author xinglu
 * @version 1.0.0
 * @since 2024/06/12 15:21
 */
public class SensorsDataBaseOptionRules {
    public static OptionRule.Builder getBaseOptionRuleBuilder() {
        return OptionRule.builder()
                .required(SensorsDataOptions.ENTITY_NAME, SensorsDataOptions.RECORD_TYPE)
                .conditional(
                        SensorsDataOptions.ENTITY_NAME,
                        "users",
                        SensorsDataOptions.SCHEMA,
                        SensorsDataOptions.DISTINCT_ID_COLUMN,
                        SensorsDataOptions.IDENTITY_FIELDS,
                        SensorsDataOptions.PROPERTY_FIELDS)
                .conditional(
                        SensorsDataOptions.RECORD_TYPE,
                        "events",
                        SensorsDataOptions.TIME_COLUMN,
                        SensorsDataOptions.EVENT_NAME)
                .conditional(
                        SensorsDataOptions.RECORD_TYPE,
                        "details",
                        SensorsDataOptions.DETAIL_ID_COLUMN)
                .conditional(
                        SensorsDataOptions.RECORD_TYPE,
                        "items",
                        SensorsDataOptions.ITEM_ID_COLUMN,
                        SensorsDataOptions.ITEM_TYPE_COLUMN)
                .optional(SensorsDataOptions.TIME_FREE);
    }
}
