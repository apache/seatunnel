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

package org.apache.seatunnel.connectors.seatunnel.influxdb.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

public class InfluxDBSourceOptions extends InfluxDBCommonOptions {

    public static final Option<String> SQL =
            Options.key("sql")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the influxdb server query sql");

    public static final Option<String> SQL_WHERE =
            Options.key("where")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the influxdb server query sql where condition");

    public static final Option<String> SPLIT_COLUMN =
            Options.key("split_column")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the influxdb column which is used as split key");

    public static final Option<Integer> PARTITION_NUM =
            Options.key("partition_num")
                    .intType()
                    .defaultValue(0)
                    .withDescription("the influxdb server partition num");

    public static final Option<Integer> UPPER_BOUND =
            Options.key("upper_bound")
                    .intType()
                    .noDefaultValue()
                    .withDescription("the influxdb server upper bound");

    public static final Option<Integer> LOWER_BOUND =
            Options.key("lower_bound")
                    .intType()
                    .noDefaultValue()
                    .withDescription("the influxdb server lower bound");
}
