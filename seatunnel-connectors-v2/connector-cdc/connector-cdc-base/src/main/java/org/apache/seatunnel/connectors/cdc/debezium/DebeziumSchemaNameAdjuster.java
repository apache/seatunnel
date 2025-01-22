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

package org.apache.seatunnel.connectors.cdc.debezium;

import org.apache.kafka.connect.errors.ConnectException;

import io.debezium.util.SchemaNameAdjuster;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DebeziumSchemaNameAdjuster {
    private static final SchemaNameAdjuster.ReplacementOccurred HANDLER =
            (original, replacement, conflictsWith) -> {
                if (conflictsWith != null) {
                    String msg =
                            "The Kafka Connect schema name '"
                                    + original
                                    + "' is not a valid Avro schema name and its replacement '"
                                    + replacement
                                    + "' conflicts with another different schema '"
                                    + conflictsWith
                                    + "'";
                    log.error(msg);
                    throw new ConnectException(msg);
                } else {
                    log.warn(
                            "The Kafka Connect schema name '{}' is not a valid Avro schema name, so replacing with '{}'",
                            original,
                            replacement);
                }
            };

    private static final SchemaNameAdjuster.ReplacementFunction REPLACE_CHAR_HANDLER =
            invalid -> {
                // Support for Chinese characters
                if (Character.isIdeographic(invalid)) {
                    return String.valueOf(invalid);
                }
                return "_";
            };

    public static SchemaNameAdjuster create() {
        return (original) ->
                SchemaNameAdjuster.validFullname(
                        original, REPLACE_CHAR_HANDLER, HANDLER.firstTimeOnly());
    }
}
