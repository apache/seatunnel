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
import org.apache.seatunnel.connectors.cdc.base.option.SourceOptions;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.MongodbIncrementalSourceFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

public class MongodbIncrementalSourceFactoryTest {
    @Test
    public void testOptionRule() {
        Assertions.assertNotNull((new MongodbIncrementalSourceFactory()).optionRule());
    }

    @Test
    public void testWithUnsupportedStartUpMode() {
        MongodbIncrementalSourceFactory mongodbIncrementalSourceFactory =
                new MongodbIncrementalSourceFactory();
        mongodbIncrementalSourceFactory.optionRule().getOptionalOptions().stream()
                .filter((option) -> option.key().equals(SourceOptions.STARTUP_MODE_KEY))
                .forEach(
                        (option) -> {
                            Assertions.assertIterableEquals(
                                    Arrays.asList(StartupMode.INITIAL, StartupMode.TIMESTAMP),
                                    ((SingleChoiceOption<StartupMode>) option).getOptionValues());
                        });
    }
}
