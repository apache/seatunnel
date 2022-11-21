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

package org.apache.seatunnel.connectors.seatunnel.cdc.mysql.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

public class MySqlSourceOptions {
    public static final Option<String> SERVER_ID =
        Options.key("server-id")
            .stringType()
            .noDefaultValue()
            .withDescription("A numeric ID or a numeric ID range of this database client, "
                + "The numeric ID syntax is like '5400', the numeric ID range syntax "
                + "is like '5400-5408', The numeric ID range syntax is recommended when "
                + "'scan.incremental.snapshot.enabled' enabled. Every ID must be unique across all "
                + "currently-running database processes in the MySQL cluster. This connector"
                + " joins the MySQL  cluster as another server (with this unique ID) "
                + "so it can read the binlog. By default, a random number is generated between"
                + " 5400 and 6400, though we recommend setting an explicit value.");
}
