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

package org.apache.seatunnel.connectors.seatunnel.amazonkinesis;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

/** Native Kinesis producer settings. */
public final class AmazonKinesisSinkOptions {
    public static final Option<String> FORMAT =
            Options.key("format")
                    .stringType()
                    .defaultValue("json")
                    .withDescription(
                            "Payload format: json for the whole input row, binary or text for a selected field");
    public static final Option<String> DATA_FIELD =
            Options.key("data_field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Payload column for binary or text");
    public static final Option<String> PARTITION_KEY_FIELD =
            Options.key("partition_key_field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("String column for the partition key");
    public static final Option<String> PARTITION_KEY =
            Options.key("partition_key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Fixed partition key. When no key is specified a random key is generated");
    public static final Option<Integer> BATCH_SIZE =
            Options.key("batch_size")
                    .intType()
                    .defaultValue(100)
                    .withDescription("Maximum records in one PutRecords request, 1 to 500");
    public static final Option<Integer> BATCH_BYTES =
            Options.key("batch_bytes")
                    .intType()
                    .defaultValue(5242880)
                    .withDescription(
                            "Maximum total payload and key bytes per request, up to 10 MiB");
    public static final Option<Integer> RECORD_BYTES =
            Options.key("record_bytes")
                    .intType()
                    .defaultValue(1048576)
                    .withDescription(
                            "Maximum payload and key bytes per record. Set above 1 MiB only on a stream configured for large records");
    public static final Option<Integer> PARTIAL_ATTEMPTS =
            Options.key("partial_attempts")
                    .intType()
                    .defaultValue(4)
                    .withDescription("Maximum attempts per explicitly failed record, 1 to 8");

    public static final Option<Boolean> PRESERVE_ORDER =
            Options.key("preserve_order")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Use serial PutRecord and per-key sequence chaining for this writer session");

    private AmazonKinesisSinkOptions() {}
}
