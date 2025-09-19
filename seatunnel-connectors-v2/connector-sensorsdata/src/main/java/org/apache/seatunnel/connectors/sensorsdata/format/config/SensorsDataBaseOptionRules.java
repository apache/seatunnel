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

package org.apache.seatunnel.connectors.sensorsdata.format.config;

import org.apache.seatunnel.api.configuration.util.OptionRule;

import lombok.experimental.UtilityClass;

@UtilityClass
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
