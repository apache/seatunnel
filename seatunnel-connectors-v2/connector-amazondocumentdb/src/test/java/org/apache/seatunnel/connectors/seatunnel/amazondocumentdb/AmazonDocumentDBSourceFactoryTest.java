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

package org.apache.seatunnel.connectors.seatunnel.amazondocumentdb;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.connectors.seatunnel.amazondocumentdb.config.AmazonDocumentDBSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.amazondocumentdb.source.AmazonDocumentDBSourceFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

public class AmazonDocumentDBSourceFactoryTest {

    @Test
    public void testSourceFactoryOptions() {
        AmazonDocumentDBSourceFactory sourceFactory = new AmazonDocumentDBSourceFactory();
        OptionRule optionRule = sourceFactory.optionRule();
        List<Option<?>> requiredOptions =
                optionRule.getRequiredOptions().stream()
                        .flatMap(requiredOption -> requiredOption.getOptions().stream())
                        .collect(Collectors.toList());

        Assertions.assertEquals("AmazonDocumentDB", sourceFactory.factoryIdentifier());
        Assertions.assertTrue(requiredOptions.contains(AmazonDocumentDBSourceOptions.URI));
        Assertions.assertTrue(requiredOptions.contains(AmazonDocumentDBSourceOptions.DATABASE));
        Assertions.assertTrue(requiredOptions.contains(AmazonDocumentDBSourceOptions.COLLECTION));
        Assertions.assertTrue(requiredOptions.contains(ConnectorCommonOptions.SCHEMA));
        Assertions.assertTrue(
                optionRule.getOptionalOptions().contains(AmazonDocumentDBSourceOptions.TLS));
        Assertions.assertTrue(
                optionRule
                        .getOptionalOptions()
                        .contains(AmazonDocumentDBSourceOptions.TLS_CA_FILE));
    }
}
