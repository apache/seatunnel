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

    @Option(name = "short-value", description = "shortValue")
    private Short shortValue;

    @Option
    private Integer intValue;

    @Option(description = "longValue")
    private Long longValue;

    @Option(description = "floatValue")
    private Float floatValue;

    @Option(description = "doubleValue")
    private Double doubleValue;

    @Option(description = "stringValue")
    private String stringValue = "default string";

    @Option(description = "booleanValue")
    private Boolean booleanValue = true;

    @Option(description = "byteValue")
    private Byte byteValue;

    @Option(description = "charValue")
    private Character charValue;

    @Option(description = "enumValue")
    private TestOptionConfigEnum enumValue = TestOptionConfigEnum.KEY2;

    @Option(description = "objectValue")
    private TestOptionConfig objectValue;

    @Option(description = "listValue")
    private List<TestOptionConfig> listValue;

    @Option(description = "mapValue")
    private Map<String, String> mapValue;

}
