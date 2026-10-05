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

package org.apache.seatunnel.api.options;

import org.apache.seatunnel.api.annotation.Experimental;
import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.HashMap;
import java.util.Map;

public class SinkConnectorCommonOptions extends ConnectorCommonOptions {

    @Experimental
    public static Option<Integer> MULTI_TABLE_SINK_REPLICA =
            Options.key("multi_table_sink_replica")
                    .intType()
                    .defaultValue(1)
                    .withDescription("The replica number of multi table sink writer");

    @Experimental
    public static Option<Map<String, String>> TABLE_OPTIONS =
            Options.key("table_options")
                    .mapType()
                    .defaultValue(new HashMap<>())
                    .withDescription(
                            "Experimental sink-specific table options applied when auto-creating "
                                    + "target tables during SaveMode. Allowed keys and semantics are "
                                    + "defined and validated by each sink connector and/or database "
                                    + "dialect; see the connector documentation for supported options.");
}
