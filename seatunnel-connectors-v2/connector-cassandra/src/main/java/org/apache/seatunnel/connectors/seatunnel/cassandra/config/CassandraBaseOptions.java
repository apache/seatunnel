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

package org.apache.seatunnel.connectors.seatunnel.cassandra.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

public class CassandraBaseOptions {

    public static final Integer DEFAULT_BATCH_SIZE = 5000;

    public static final Option<String> HOST =
            Options.key("host").stringType().noDefaultValue().withDescription("");

    public static final Option<String> KEYSPACE =
            Options.key("keyspace").stringType().noDefaultValue().withDescription("");

    public static final Option<String> USERNAME =
            Options.key("username").stringType().noDefaultValue().withDescription("");
    public static final Option<String> PASSWORD =
            Options.key("password").stringType().noDefaultValue().withDescription("");
    public static final Option<String> DATACENTER =
            Options.key("datacenter").stringType().defaultValue("datacenter1").withDescription("");

    public static final Option<String> CONSISTENCY_LEVEL =
            Options.key("consistency_level")
                    .stringType()
                    .defaultValue("LOCAL_ONE")
                    .withDescription("");
}
