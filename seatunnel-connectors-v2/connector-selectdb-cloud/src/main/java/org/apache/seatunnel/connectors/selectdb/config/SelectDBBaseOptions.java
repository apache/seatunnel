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

package org.apache.seatunnel.connectors.selectdb.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

public class SelectDBBaseOptions {

    public static final String IDENTIFIER = "SelectDBCloud";

    // common option
    public static final Option<String> FENODES =
            Options.key("fenodes")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("selectDB fe http address.");

    public static final Option<Integer> QUERY_PORT =
            Options.key("query-port")
                    .intType()
                    .defaultValue(9030)
                    .withDescription("selectDB query port");

    public static final Option<String> USERNAME =
            Options.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the selectDB user name.");

    public static final Option<String> PASSWORD =
            Options.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the selectDB password.");

    public static final Option<String> TABLE =
            Options.key("table").stringType().noDefaultValue().withDescription("table");

    public static final Option<String> DATABASE =
            Options.key("database").stringType().noDefaultValue().withDescription("database");

    public static final Option<Integer> SELECTDB_BATCH_SIZE =
            Options.key("selectdb.batch.size")
                    .intType()
                    .defaultValue(1024)
                    .withDescription("the batch size of the selectDB read/write.");
}
