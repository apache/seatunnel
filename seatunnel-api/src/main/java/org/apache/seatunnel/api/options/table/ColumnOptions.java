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

package org.apache.seatunnel.api.options.table;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.List;
import java.util.Map;

public interface ColumnOptions {

    // todo: how to define List<Map<String, Object>>
    Option<List<Map<String, Object>>> COLUMNS =
            Options.key("columns")
                    .type(new TypeReference<List<Map<String, Object>>>() {})
                    .noDefaultValue()
                    .withDescription("SeaTunnel Schema Columns");

    Option<String> COLUMN_NAME =
            Options.key("name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("SeaTunnel Schema Column Name");

    Option<String> TYPE =
            Options.key("type")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("SeaTunnel Schema Column Type");

    Option<Integer> COLUMN_SCALE =
            Options.key("columnScale")
                    .intType()
                    .noDefaultValue()
                    .withDescription("SeaTunnel Schema Column scale");

    Option<Long> COLUMN_LENGTH =
            Options.key("columnLength")
                    .longType()
                    .defaultValue(0L)
                    .withDescription("SeaTunnel Schema Column Length");

    Option<Boolean> NULLABLE =
            Options.key("nullable")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("SeaTunnel Schema Column Nullable");

    Option<Object> DEFAULT_VALUE =
            Options.key("defaultValue")
                    .objectType(Object.class)
                    .noDefaultValue()
                    .withDescription("SeaTunnel Schema Column Default Value");

    Option<String> COLUMN_COMMENT =
            Options.key("comment")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("SeaTunnel Schema Column Comment");
}
