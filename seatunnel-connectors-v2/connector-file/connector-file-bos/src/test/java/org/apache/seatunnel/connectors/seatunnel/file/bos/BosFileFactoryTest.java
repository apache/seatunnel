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

package org.apache.seatunnel.connectors.seatunnel.file.bos;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.connectors.seatunnel.file.bos.sink.BosFileSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.file.bos.source.BosFileSourceFactory;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSourceOptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class BosFileFactoryTest {

    @Test
    void optionRule() {
        Assertions.assertNotNull((new BosFileSourceFactory()).optionRule());
        Assertions.assertNotNull((new BosFileSinkFactory()).optionRule());
    }

    @Test
    void consumedOptionsAreDeclared() {
        OptionRule optionRule = (new BosFileSourceFactory()).optionRule();
        Assertions.assertTrue(
                optionRule.getOptionalOptions().contains(FileBaseSourceOptions.READ_PARTITIONS));
        Assertions.assertTrue(
                optionRule
                        .getOptionalOptions()
                        .contains(FileBaseSourceOptions.CSV_USE_HEADER_LINE));
        Assertions.assertTrue(
                optionRule.getOptionalOptions().contains(FileBaseSourceOptions.BINARY_CHUNK_SIZE));
        Assertions.assertTrue(
                optionRule
                        .getOptionalOptions()
                        .contains(FileBaseSourceOptions.BINARY_COMPLETE_FILE_MODE));
        Assertions.assertTrue(
                optionRule
                        .getOptionalOptions()
                        .contains(FileBaseSourceOptions.FILE_FILTER_MODIFIED_START));
        Assertions.assertTrue(
                optionRule
                        .getOptionalOptions()
                        .contains(FileBaseSourceOptions.FILE_FILTER_MODIFIED_END));
        Assertions.assertTrue(
                optionRule
                        .getOptionalOptions()
                        .contains(FileBaseSourceOptions.SORT_FILES_BY_MOD_TIME));
    }
}
