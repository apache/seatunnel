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

package mongodb.source;

import org.apache.seatunnel.api.configuration.SingleChoiceOption;
import org.apache.seatunnel.connectors.cdc.base.config.StartupConfig;
import org.apache.seatunnel.connectors.cdc.base.option.SourceOptions;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.MongodbIncrementalSourceFactory;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceConfigProvider;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.exception.MongodbConnectorException;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.source.offset.ChangeStreamOffset;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.source.offset.ChangeStreamOffsetFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

public class MongodbIncrementalSourceFactoryTest {
    @Test
    public void testOptionRule() {
        Assertions.assertNotNull((new MongodbIncrementalSourceFactory()).optionRule());
    }

    @Test
    public void testSupportedStartUpModes() {
        MongodbIncrementalSourceFactory mongodbIncrementalSourceFactory =
                new MongodbIncrementalSourceFactory();
        mongodbIncrementalSourceFactory.optionRule().getOptionalOptions().stream()
                .filter((option) -> option.key().equals(SourceOptions.STARTUP_MODE_KEY))
                .forEach(
                        (option) -> {
                            Assertions.assertIterableEquals(
                                    Arrays.asList(
                                            StartupMode.INITIAL,
                                            StartupMode.LATEST,
                                            StartupMode.TIMESTAMP),
                                    ((SingleChoiceOption<StartupMode>) option).getOptionValues());
                        });
    }

    @Test
    public void testSourceConfigBuilderAcceptsLatestStartupMode() {
        // Regression for the real source-assembly path: the builder used to reject
        // StartupMode.LATEST at runtime even though the option rule advertised it.
        MongodbSourceConfig config =
                MongodbSourceConfigProvider.newBuilder()
                        .hosts("localhost:27017")
                        .startupOptions(new StartupConfig(StartupMode.LATEST, null, null, null))
                        .validate()
                        .create(0);

        Assertions.assertEquals(StartupMode.LATEST, config.getStartupConfig().getStartupMode());
    }

    @Test
    public void testSourceConfigBuilderRejectsUnsupportedStartupMode() {
        Assertions.assertThrows(
                MongodbConnectorException.class,
                () ->
                        MongodbSourceConfigProvider.newBuilder()
                                .startupOptions(
                                        new StartupConfig(StartupMode.EARLIEST, null, null, null)));
    }

    @Test
    public void testLatestStartupModeResolvesToLatestChangeStreamOffset() {
        StartupConfig startupConfig = new StartupConfig(StartupMode.LATEST, null, null, null);

        Offset startupOffset = startupConfig.getStartupOffset(new ChangeStreamOffsetFactory());

        Assertions.assertInstanceOf(ChangeStreamOffset.class, startupOffset);
        // The latest offset is a current-time change-stream position, not a resume token from a
        // snapshot: starting here means only changes made after the job starts are consumed.
        Assertions.assertNotNull(((ChangeStreamOffset) startupOffset).getTimestamp());
        Assertions.assertNull(((ChangeStreamOffset) startupOffset).getResumeToken());
    }
}
