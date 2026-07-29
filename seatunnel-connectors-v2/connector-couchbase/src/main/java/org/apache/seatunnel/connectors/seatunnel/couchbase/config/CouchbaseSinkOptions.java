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

package org.apache.seatunnel.connectors.seatunnel.couchbase.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.List;

/** Configuration options specific to the Couchbase sink connector. */
public class CouchbaseSinkOptions extends CouchbaseConfig {

    /**
     * Maximum number of rows buffered before a batch write is triggered.
     *
     * <p>A value of {@code -1} disables size-based flushing.
     */
    public static final Option<Integer> BUFFER_FLUSH_MAX_ROWS =
            Options.key("buffer-flush.max-rows")
                    .intType()
                    .defaultValue(1000)
                    .withDescription(
                            "The maximum number of buffered rows per batch write request."
                                    + " Use -1 to disable size-based flushing.");

    /**
     * Maximum time (ms) between two consecutive batch writes.
     *
     * <p>A value of {@code -1} disables interval-based flushing.
     */
    public static final Option<Long> BUFFER_FLUSH_INTERVAL =
            Options.key("buffer-flush.interval")
                    .longType()
                    .defaultValue(30000L)
                    .withDescription(
                            "The maximum interval between batch write requests, in milliseconds."
                                    + " Use -1 to disable interval-based flushing.");

    /** Number of retry attempts on transient write failures before giving up. */
    public static final Option<Integer> RETRY_MAX =
            Options.key("retry.max")
                    .intType()
                    .defaultValue(3)
                    .withDescription(
                            "The maximum number of retries if writing records to Couchbase fails.");

    /** Time to wait between retry attempts, in milliseconds. */
    public static final Option<Long> RETRY_INTERVAL =
            Options.key("retry.interval")
                    .longType()
                    .defaultValue(1000L)
                    .withDescription(
                            "The retry interval in milliseconds if writing records to Couchbase fails.");

    /**
     * When {@code true}, existing documents are replaced (upserted) instead of being inserted.
     * Requires {@link #PRIMARY_KEY} to be set so that the document key can be built.
     */
    public static final Option<Boolean> UPSERT_ENABLE =
            Options.key("upsert-enable")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to write documents via upsert (replace) mode."
                                    + " When false, documents are inserted and duplicate keys"
                                    + " will cause an error.");

    /**
     * Field names whose values are assembled into the Couchbase document key using a
     * <em>length-prefixed canonical encoding</em>. Each component is encoded as {@code
     * <len>:<value>} and components are separated by {@code #}, e.g. field values {@code "a_b"} and
     * {@code "c"} produce the key {@code "3:a_b#1:c"}. This encoding is collision-free regardless
     * of the character content of the values. When not provided a random UUID is used as the
     * document key.
     */
    public static final Option<List<String>> PRIMARY_KEY =
            Options.key("primary-key")
                    .listType()
                    .noDefaultValue()
                    .withDescription(
                            "The field names used to build the Couchbase document key."
                                    + " Each value is encoded as '<len>:<value>' and components"
                                    + " are separated by '#' (e.g. field values 'a_b' and 'c'"
                                    + " produce the key '3:a_b#1:c'). This length-prefixed"
                                    + " encoding is collision-free. When not set, a random UUID"
                                    + " is used as the document key.");
}
