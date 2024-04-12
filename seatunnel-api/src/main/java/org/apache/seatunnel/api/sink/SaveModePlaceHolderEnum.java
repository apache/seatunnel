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

public enum SaveModePlaceHolderEnum {
    ROWTYPE_PRIMARY_KEY("primaryKeys"),
    ROWTYPE_UNIQUE_KEY("uniqueKeys"),
    ROWTYPE_FIELDS("fields"),
    TABLE_NAME("tableName"),
    DATABASE("database");

    private String keyValue;

    SaveModePlaceHolderEnum(String keyValue) {
        this.keyValue = keyValue;
    }

    public static String getKeyValue(String sqlKeyTypePlaceholder) {
        return SaveModePlaceHolderEnum.valueOf(sqlKeyTypePlaceholder.toUpperCase()).keyValue;
    }

    public String getPlaceHolder() {
        return this.name().toLowerCase();
    }
}
