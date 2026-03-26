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

package org.apache.seatunnel.connectors.seatunnel.file.ftp;

import org.apache.seatunnel.api.configuration.util.Expression;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.RequiredOption;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileSyncMode;
import org.apache.seatunnel.connectors.seatunnel.file.ftp.sink.FtpFileSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.file.ftp.source.FtpFileSourceFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class FtpFileFactoryTest {

    @Test
    void optionRule() {
        OptionRule optionRule = (new FtpFileSourceFactory()).optionRule();
        Assertions.assertNotNull(optionRule);
        Assertions.assertTrue(
                optionRule.getOptionalOptions().contains(FileBaseSourceOptions.SYNC_MODE));
        Assertions.assertTrue(
                optionRule.getOptionalOptions().contains(FileBaseSourceOptions.TARGET_HADOOP_CONF));
        Assertions.assertTrue(
                optionRule.getOptionalOptions().contains(FileBaseSourceOptions.UPDATE_STRATEGY));
        Assertions.assertTrue(
                optionRule.getOptionalOptions().contains(FileBaseSourceOptions.COMPARE_MODE));

        Expression expectExpression =
                Expression.of(FileBaseSourceOptions.SYNC_MODE, FileSyncMode.UPDATE);
        Assertions.assertTrue(
                optionRule.getRequiredOptions().stream()
                        .filter(RequiredOption.ConditionalRequiredOptions.class::isInstance)
                        .map(RequiredOption.ConditionalRequiredOptions.class::cast)
                        .filter(
                                required ->
                                        required.getOptions()
                                                .contains(FileBaseSourceOptions.TARGET_PATH))
                        .anyMatch(required -> expectExpression.equals(required.getExpression())));
        Assertions.assertNotNull((new FtpFileSinkFactory()).optionRule());
    }
}
