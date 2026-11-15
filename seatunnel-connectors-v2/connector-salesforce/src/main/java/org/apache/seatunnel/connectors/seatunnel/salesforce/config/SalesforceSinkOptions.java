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

package org.apache.seatunnel.connectors.seatunnel.salesforce.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

public final class SalesforceSinkOptions {
    private SalesforceSinkOptions() {}

    public static final Option<String> EXTERNAL_ID_FIELD =
            Options.key("external_id_field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Salesforce external ID field present in every input row.");
    public static final Option<Integer> BATCH_SIZE =
            Options.key("batch_size")
                    .intType()
                    .defaultValue(200)
                    .withDescription("Maximum records per REST collection upsert, from 1 to 200.");
    public static final Option<Integer> BATCH_MAX_BYTES =
            Options.key("batch_max_bytes")
                    .intType()
                    .defaultValue(1024 * 1024)
                    .withDescription(
                            "Maximum serialized request size in bytes, including the JSON envelope. This is a client memory bound, not a Salesforce API limit.");
    public static final Option<Integer> MAX_RETRIES =
            Options.key("max_retries")
                    .intType()
                    .defaultValue(3)
                    .withDescription(
                            "Maximum retries per upsert request, from 0 to 10. Record-level failures are not retried.");
    public static final Option<Long> RETRY_INTERVAL_MS =
            Options.key("retry_interval_ms")
                    .longType()
                    .defaultValue(1000L)
                    .withDescription("Delay between retries, from 0 to 60000 milliseconds.");
}
