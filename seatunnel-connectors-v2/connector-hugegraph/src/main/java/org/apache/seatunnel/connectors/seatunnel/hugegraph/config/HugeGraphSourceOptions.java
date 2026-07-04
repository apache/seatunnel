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

package org.apache.seatunnel.connectors.seatunnel.hugegraph.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.List;

public class HugeGraphSourceOptions {

    public static final Option<String> LABEL =
            Options.key("label")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The vertex or edge label to read from HugeGraph");

    public static final Option<LabelType> TYPE =
            Options.key("type")
                    .enumType(LabelType.class)
                    .noDefaultValue()
                    .withDescription("The type of graph element to read: VERTEX or EDGE");

    public static final Option<List<String>> PROPERTIES =
            Options.key("properties")
                    .listType()
                    .noDefaultValue()
                    .withDescription(
                            "The list of property names to read. If not specified, all properties will be read.");

    public static final Option<Integer> PAGE_SIZE =
            Options.key("page_size")
                    .intType()
                    .defaultValue(500)
                    .withDescription("The number of records to fetch per page from HugeGraph");

    public static final Option<Integer> LIMIT =
            Options.key("limit")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "The maximum number of records to read. If not specified, all records will be read.");

    public enum LabelType {
        VERTEX,
        EDGE
    }
}
