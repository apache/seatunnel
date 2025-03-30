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

package org.apache.seatunnel.connectors.seatunnel.amazondynamodb.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

public class AmazonDynamoDBSourceOptions extends AmazonDynamoDBBaseOptions {

    public static final Option<Integer> SCAN_ITEM_LIMIT =
            Options.key("scan_item_limit")
                    .intType()
                    .defaultValue(1)
                    .withDescription("number of item each scan request should return");

    public static final Option<Integer> PARALLEL_SCAN_THREADS =
            Options.key("parallel_scan_threads")
                    .intType()
                    .defaultValue(2)
                    .withDescription("number of logical segments for parallel scan");
}
