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

package org.apache.seatunnel.connectors.seatunnel.kudu.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import org.apache.kudu.client.SessionConfiguration;

public class KuduSinkOptions extends KuduBaseOptions {

    public static final Option<KuduSinkConfig.SaveMode> SAVE_MODE =
            Options.key("save_mode")
                    .enumType(KuduSinkConfig.SaveMode.class)
                    .defaultValue(KuduSinkConfig.SaveMode.APPEND)
                    .withDescription("Storage mode,append is now supported");

    public static final Option<String> FLUSH_MODE =
            Options.key("session_flush_mode")
                    .stringType()
                    .defaultValue(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC.name())
                    .withDescription("Kudu flush mode. Default AUTO_FLUSH_SYNC");

    public static final Option<Integer> BATCH_SIZE =
            Options.key("batch_size")
                    .intType()
                    .defaultValue(1024)
                    .withDescription(
                            "the flush max size (includes all append, upsert and delete records), over this number"
                                    + " of records, will flush data. The default value is 100.");

    public static final Option<Integer> BUFFER_FLUSH_INTERVAL =
            Options.key("buffer_flush_interval")
                    .intType()
                    .defaultValue(10000)
                    .withDescription(
                            "the flush interval mills, over this time, asynchronous threads will flush data. The "
                                    + "default value is 1s.");

    public static final Option<Boolean> IGNORE_NOT_FOUND =
            Options.key("ignore_not_found")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("if true, ignore all not found rows");

    public static final Option<Boolean> IGNORE_DUPLICATE =
            Options.key("ignore_not_duplicate")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("if true, ignore all dulicate rows");
}
