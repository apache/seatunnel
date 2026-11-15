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

package org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConditionExtension;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.connectors.cdc.base.config.StartupConfig;
import org.apache.seatunnel.connectors.cdc.base.config.StopConfig;
import org.apache.seatunnel.connectors.cdc.base.option.SourceOptions;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.cdc.base.option.StopMode;
import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.source.offset.ChangeStreamOffsetFactory;

/**
 * Rejects a {@code stop.timestamp} that does not resolve to a change-stream position after the
 * configured {@code startup.timestamp}.
 *
 * <p>The two millisecond values are compared as the positions they resolve to, because MongoDB
 * change-stream timestamps only carry second precision: two different millisecond values inside the
 * same second resolve to the same position and are rejected as well. The check only applies when
 * the startup mode is {@code timestamp}; the stop mode is {@code timestamp} whenever the condition
 * is evaluated.
 */
public class MongodbTimestampRangeCondition implements ConditionExtension<Long> {

    @Override
    public String description() {
        return "must resolve to a change-stream position after '"
                + SourceOptions.STARTUP_TIMESTAMP.key()
                + "' when '"
                + SourceOptions.STARTUP_MODE_KEY
                + "' is 'timestamp'";
    }

    @Override
    public boolean evaluate(ReadonlyConfig config, Long stopTimestamp)
            throws OptionValidationException {
        StartupMode startupMode =
                config.getOptional(MongodbIncrementalSourceOptions.STARTUP_MODE)
                        .orElseGet(MongodbIncrementalSourceOptions.STARTUP_MODE::defaultValue);
        Long startupTimestamp = config.getOptional(SourceOptions.STARTUP_TIMESTAMP).orElse(null);
        if (stopTimestamp == null
                || startupTimestamp == null
                || startupMode != StartupMode.TIMESTAMP) {
            return true;
        }
        ChangeStreamOffsetFactory offsetFactory = new ChangeStreamOffsetFactory();
        Offset startupOffset =
                new StartupConfig(StartupMode.TIMESTAMP, null, null, startupTimestamp)
                        .getStartupOffset(offsetFactory);
        Offset stopOffset =
                new StopConfig(StopMode.TIMESTAMP, null, null, stopTimestamp)
                        .getStopOffset(offsetFactory);
        if (stopOffset.compareTo(startupOffset) <= 0) {
            throw new OptionValidationException(
                    String.format(
                            "Invalid timestamp range: stop.timestamp (%d) resolves to MongoDB "
                                    + "position %s, which must be after startup.timestamp (%d) at "
                                    + "position %s. MongoDB change-stream timestamps have second "
                                    + "precision.",
                            stopTimestamp, stopOffset, startupTimestamp, startupOffset));
        }
        return true;
    }
}
