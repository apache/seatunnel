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

package org.apache.seatunnel.transform.fieldmapper;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

@Getter
@Setter
public class FieldMapperTransformConfig implements Serializable {
    public static final Option<Map<String, String>> FIELD_MAPPER =
            Options.key("field_mapper")
                    .mapType()
                    .noDefaultValue()
                    .withDescription(
                            "Specify the field mapping relationship between input and output");

    private Map<String, String> fieldMapper = new LinkedHashMap<>();

    public static FieldMapperTransformConfig of(ReadonlyConfig config) {
        FieldMapperTransformConfig fieldMapperTransformConfig = new FieldMapperTransformConfig();
        fieldMapperTransformConfig.setFieldMapper(config.get(FIELD_MAPPER));
        return fieldMapperTransformConfig;
    }
}
