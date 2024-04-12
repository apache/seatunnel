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

package org.apache.seatunnel.api.sink;

import java.util.Arrays;
import java.util.Optional;

public enum SaveModePlaceHolderEnum {
    ROWTYPE_PRIMARY_KEY("rowtype_primary_key", "primaryKeys"),
    ROWTYPE_UNIQUE_KEY("rowtype_unique_key", "uniqueKeys"),
    ROWTYPE_FIELDS("rowtype_fields", "fields"),
    TABLE_NAME("table_name", "tableName"),
    DATABASE("database", "database");

    private String keyValue;
    private String actualValue;

    private static final String REPLACE_PLACE_HOLDER = "\\$\\{%s\\}";
    private static final String PLACE_HOLDER = "${%s}";

    SaveModePlaceHolderEnum(String keyValue, String actualValue) {
        this.keyValue = keyValue;
        this.actualValue = actualValue;
    }

    public static String getActualValueByPlaceHolder(String placeholder) {
        Optional<SaveModePlaceHolderEnum> saveModePlaceHolderEnumOptional =
                Arrays.stream(SaveModePlaceHolderEnum.values())
                        .filter(
                                saveModePlaceHolderEnum ->
                                        placeholder.equals(
                                                saveModePlaceHolderEnum.getPlaceHolder()))
                        .findFirst();
        if (saveModePlaceHolderEnumOptional.isPresent()) {
            return saveModePlaceHolderEnumOptional.get().actualValue;
        }
        throw new RuntimeException(String.format("Not support the placeholder: %s", placeholder));
    }

    public String getPlaceHolderKeyValue() {
        return this.keyValue;
    }

    public String getPlaceHolder() {
        return String.format(PLACE_HOLDER, getPlaceHolderKeyValue());
    }

    public String getReplacePlaceHolder() {
        return String.format(REPLACE_PLACE_HOLDER, getPlaceHolderKeyValue());
    }
}
