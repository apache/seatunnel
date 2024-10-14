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

package org.apache.seatunnel.transform.rowkind;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
public class RowKindExtractorTransformConfig implements Serializable {

    public static final String PLUGIN_NAME = "RowKindExtractor";

    public static final Option<String> CUSTOM_FIELD_NAME =
            Options.key("custom_field_name")
                    .stringType()
                    .defaultValue("row_kind")
                    .withDescription("Custom field name of the RowKind field");

    public static final Option<RowKindExtractorTransformType> TRANSFORM_TYPE =
            Options.key("transform_type")
                    .enumType(RowKindExtractorTransformType.class)
                    .defaultValue(RowKindExtractorTransformType.SHORT)
                    .withDescription("transform RowKind field value format");
}
