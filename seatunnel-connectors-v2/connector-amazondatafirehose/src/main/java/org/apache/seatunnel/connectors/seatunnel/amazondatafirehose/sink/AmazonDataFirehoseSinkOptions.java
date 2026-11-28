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

package org.apache.seatunnel.connectors.seatunnel.amazondatafirehose.sink;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

/**
 * Defines stable DirectPut sink settings, including credential, record and whole-flush limits.
 * These option keys are the user contract shared by factory validation and writer construction.
 */
public final class AmazonDataFirehoseSinkOptions {
    public static final Option<String> DELIVERY_STREAM =
            Options.key("delivery_stream")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Name of an existing Amazon Data Firehose DirectPut delivery stream.");
    public static final Option<String> REGION =
            Options.key("region")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("AWS Region for Firehose endpoint resolution and signing.");
    public static final Option<String> ACCESS_KEY =
            Options.key("access_key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "AWS access key ID. If omitted, use the worker AWS environment credentials.");
    public static final Option<String> SECRET_KEY =
            Options.key("secret_key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "AWS secret access key for an explicitly configured access key.");
    public static final Option<String> TOKEN =
            Options.key("token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional AWS session token for an explicitly configured credential pair.");
    public static final Option<String> ENDPOINT_URL =
            Options.key("endpoint_url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Trusted HTTPS origin override for a Firehose-compatible endpoint.");
    public static final Option<Boolean> ALLOW_INSECURE_HTTP =
            Options.key("allow_insecure_http")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Permit HTTP only for an explicit trusted, isolated development endpoint.");
    public static final Option<Format> FORMAT =
            Options.key("format")
                    .enumType(Format.class)
                    .defaultValue(Format.JSON)
                    .withDescription(
                            "JSON or DELIMITED serializes the row with LF. JSON_LINES validates an encoded JSON object. RAW preserves bytes.");
    public static final Option<String> FIELD_DELIMITER =
            Options.key("field_delimiter")
                    .stringType()
                    .defaultValue(",")
                    .withDescription("Field separator used by DELIMITED format.");
    public static final Option<Integer> MAX_RECORD_BYTES =
            Options.key("max_record_bytes")
                    .intType()
                    .defaultValue(1000 * 1024)
                    .withDescription(
                            "Maximum pre-base64 record bytes including the delimiter, up to 1000 KiB.");
    public static final Option<Integer> BATCH_SIZE =
            Options.key("batch_size")
                    .intType()
                    .defaultValue(100)
                    .withDescription("Maximum records per batch, from 1 to 500.");
    public static final Option<Integer> BATCH_TIMEOUT_MS =
            Options.key("batch_timeout_ms")
                    .intType()
                    .defaultValue(30000)
                    .withDescription(
                            "Total time per flush in milliseconds, including failed-subset retries.");
    public static final Option<Integer> MAX_RETRIES =
            Options.key("max_retries")
                    .intType()
                    .defaultValue(2)
                    .withDescription(
                            "Maximum retries of entries Firehose explicitly reports as failed, from 0 to 10.");

    private AmazonDataFirehoseSinkOptions() {}

    /**
     * Selects native row serialization or an already encoded payload without changing record
     * identity. All formats except RAW append one LF, which counts toward the configured
     * record-byte limit.
     */
    public enum Format {
        JSON,
        DELIMITED,
        JSON_LINES,
        RAW
    }
}
