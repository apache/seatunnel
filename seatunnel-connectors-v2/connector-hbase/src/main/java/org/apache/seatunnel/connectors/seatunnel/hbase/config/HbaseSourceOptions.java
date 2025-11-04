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

package org.apache.seatunnel.connectors.seatunnel.hbase.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

public class HbaseSourceOptions extends HbaseBaseOptions {

    public static final Option<String> START_ROW_KEY =
            Options.key("start_rowkey")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Hbase scan start rowkey");

    public static final Option<String> END_ROW_KEY =
            Options.key("end_rowkey")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Hbase scan end rowkey");

    public static final Option<Boolean> IS_BINARY_ROW_KEY =
            Options.key("is_binary_rowkey")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("is binary rowkey");

    public static final Option<Boolean> HBASE_CACHE_BLOCKS_CONFIG =
            Options.key("cache_blocks")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "When it is false, data blocks are not cached. "
                                    + "When it is true, data blocks are cached. "
                                    + "This value should be set to false when scanning a large amount of data to reduce memory consumption. "
                                    + "The default value is false");

    public static final Option<Integer> HBASE_CACHING_CONFIG =
            Options.key("caching")
                    .intType()
                    .defaultValue(-1)
                    .withDescription(
                            "Set the number of rows read from the server each time can reduce the number of round trips between the client and the server, "
                                    + "thereby improving performance. The default value is -1.");

    public static final Option<Integer> HBASE_BATCH_CONFIG =
            Options.key("batch")
                    .intType()
                    .defaultValue(-1)
                    .withDescription(
                            "Set the batch size to control the maximum number of cells returned each time, "
                                    + "thereby controlling the amount of data returned by a single RPC call. The default value is -1.");
}
