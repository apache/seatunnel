/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.druid.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

public class DruidSinkOptions {
    public static final Integer BATCH_SIZE_DEFAULT = 10000;

    public static Option<String> COORDINATOR_URL =
            Options.key("coordinatorUrl")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The coordinatorUrl host and port of Druid.");

    public static Option<String> DATASOURCE =
            Options.key("datasource")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The datasource name need to write.");

    public static Option<Integer> BATCH_SIZE =
            Options.key("batchSize")
                    .intType()
                    .defaultValue(BATCH_SIZE_DEFAULT)
                    .withDescription("The batch size of the druid write.");
}
