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

package org.apache.seatunnel.connectors.seatunnel.firebase.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.List;

public class FirebaseSinkOptions extends FirebaseBaseOptions {
    public static final Option<List<String>> PRIMARY_KEYS =
            Options.key("primary_keys")
                    .listType(String.class)
                    .noDefaultValue()
                    .withDescription("Field names used to identify target node.");
    public static final Option<String> KEY_PREFIX =
            Options.key("key_prefix")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Fixed expression before key string.");
    public static final Option<String> KEY_POSTFIX =
            Options.key("key_postfix")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Fixed expression after key string.");
    public static final Option<String> KEY_DELIMITER =
            Options.key("key_delimiter")
                    .stringType()
                    .defaultValue("_")
                    .withDescription(
                            "Delimiter to concatenate values when using composite primary keys.");
    public static final Option<Integer> BATCH_SIZE =
            Options.key("batch_size")
                    .intType()
                    .defaultValue(100)
                    .withDescription(
                            "Number of records aggregated before issuing a multi-location update REST payload or batch write.");
    public static final Option<Integer> RETRY_MAX =
            Options.key("retry_max")
                    .intType()
                    .defaultValue(3)
                    .withDescription("Maximum retry attempts for failed HTTP write operations.");
    public static final Option<Boolean> IGNORE_NULL_VALUES =
            Options.key("ignore_null_values")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "If true, fields with null values are omitted from the JSON payload.");
    public static final Option<Boolean> SUPPORT_DELETES =
            Options.key("support_deletes")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether to process RowKind.DELETE and RowKind.UPDATE_BEFORE records.");
}
