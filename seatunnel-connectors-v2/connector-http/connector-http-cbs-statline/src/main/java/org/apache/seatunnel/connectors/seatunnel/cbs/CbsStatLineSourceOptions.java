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

package org.apache.seatunnel.connectors.seatunnel.cbs;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

/**
 * Defines stable configuration keys for the bounded CbsStatLine source. Both factory validation and
 * source construction use these definitions without changing defaults.
 */
final class CbsStatLineSourceOptions {
    private CbsStatLineSourceOptions() {}

    static final Option<String> DATASET =
            Options.key("dataset_id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("CBS StatLine OData v4 dataset identifier.");
    static final Option<String> BASE_URL =
            Options.key("base_url")
                    .stringType()
                    .defaultValue("https://datasets.cbs.nl")
                    .withDescription("CBS OData server origin (without a path).");
    static final Option<Integer> MAX_ROWS =
            Options.key("max_rows")
                    .intType()
                    .defaultValue(0)
                    .withDescription("Maximum rows; zero reads the entire dataset.");
}
