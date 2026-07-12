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

package org.apache.seatunnel.connectors.seatunnel.pulsar.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.seatunnel.pulsar.config.PulsarSourceOptions.StartMode;
import org.apache.seatunnel.connectors.seatunnel.pulsar.config.PulsarSourceOptions.StopMode;

import lombok.Getter;

import java.io.Serializable;
import java.util.regex.Pattern;

@Getter
/** Immutable table-level Pulsar source configuration after global defaults are merged. */
public class PulsarTableConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    private final TablePath tablePath;
    private final String topic;
    private final Pattern topicPattern;
    private final String format;
    private final StartMode startMode;
    private final Long startTimestamp;
    private final StopMode stopMode;
    private final Long stopTimestamp;
    private final PulsarSourceOptions.CursorResetStrategy resetMode;
    private final String subscriptionName;
    private final ReadonlyConfig schemaConfig;

    /**
     * Create one effective table config for source initialization.
     *
     * <p>The values in this object already reflect the final result after applying global defaults
     * and per-table overrides.
     */
    public PulsarTableConfig(
            TablePath tablePath,
            String topic,
            Pattern topicPattern,
            String format,
            StartMode startMode,
            Long startTimestamp,
            StopMode stopMode,
            Long stopTimestamp,
            PulsarSourceOptions.CursorResetStrategy resetMode,
            String subscriptionName,
            ReadonlyConfig schemaConfig) {
        this.tablePath = tablePath;
        this.topic = topic;
        this.topicPattern = topicPattern;
        this.format = format;
        this.startMode = startMode;
        this.startTimestamp = startTimestamp;
        this.stopMode = stopMode;
        this.stopTimestamp = stopTimestamp;
        this.resetMode = resetMode;
        this.subscriptionName = subscriptionName;
        this.schemaConfig = schemaConfig;
    }
}
