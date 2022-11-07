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

package org.apache.seatunnel.api.configuration.util;

import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class TestOptionConfig {

    @OptionMark(name = "short-value", description = "shortValue")
    private Short shortValue;

    @OptionMark
    private Integer intValue;

    @OptionMark(description = "longValue")
    private Long longValue;

    @OptionMark(description = "floatValue")
    private Float floatValue;

    @OptionMark(description = "doubleValue")
    private Double doubleValue;

    @OptionMark(description = "stringValue")
    private String stringValue = "default string";

    @OptionMark(description = "booleanValue")
    private Boolean booleanValue = true;

    @OptionMark(description = "byteValue")
    private Byte byteValue;

    @OptionMark(description = "charValue")
    private Character charValue;

    @OptionMark(description = "enumValue")
    private TestOptionConfigEnum enumValue = TestOptionConfigEnum.KEY2;

    @OptionMark(description = "objectValue")
    private TestOptionConfig objectValue;

    @OptionMark(description = "listValue")
    private List<TestOptionConfig> listValue;

    @OptionMark(description = "mapValue")
    private Map<String, String> mapValue;

}
