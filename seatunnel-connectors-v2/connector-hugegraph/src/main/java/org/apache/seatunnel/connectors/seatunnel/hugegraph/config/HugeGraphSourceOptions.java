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

public class HugeGraphSourceOptions {

    public static final int MIN_PAGE_SIZE = 100;
    public static final int MAX_PAGE_SIZE = 10000;

    public static final Option<String> LABEL =
            Options.key("label")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("HugeGraph vertex label or edge label to read");

    public static final Option<MappingConfig.LabelType> LABEL_TYPE =
            Options.key("label_type")
                    .enumType(MappingConfig.LabelType.class)
                    .defaultValue(MappingConfig.LabelType.VERTEX)
                    .withDescription("HugeGraph label type. Supported values are VERTEX and EDGE");

    public static final Option<Integer> PAGE_SIZE =
            Options.key("page_size")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("Records per HugeGraph page, must be in range [100, 10000]");

    public static final Option<String> TIME_ZONE =
            Options.key("time_zone")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Time zone used to convert HugeGraph DATE epoch values. "
                                    + "When omitted, the worker JVM default time zone is used for backward compatibility.");
}
