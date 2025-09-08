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

package org.apache.seatunnel.connectors.seatunnel.clickhouse.config;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Map;

@Getter
@AllArgsConstructor
public class ClickhouseType {

    public static final String STRING = "String";
    public static final String TINYINT = "Int8";
    public static final String SMALLINT = "Int16";
    public static final String INT = "Int32";
    public static final String BIGINT = "Int64";
    public static final String FLOAT = "Float32";
    public static final String BOOLEAN = "Bool";
    public static final String DOUBLE = "Float64";
    public static final String DATE = "Date";
    public static final String DateTime64 = "DateTime64";
    public static final String MAP = "Map";
    public static final String ARRAY = "Array";
    public static final String DECIMAL = "Decimal";
    private String type;
    private Map<String, Object> options;
}
