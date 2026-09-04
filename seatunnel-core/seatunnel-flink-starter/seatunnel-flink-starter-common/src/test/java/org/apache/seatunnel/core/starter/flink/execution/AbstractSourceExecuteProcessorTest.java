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

package org.apache.seatunnel.core.starter.flink.execution;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AbstractSourceExecuteProcessorTest {

    @Test
    void testSchemaEvolutionUsesSingleIncrementalReaderByDefault() {
        assertDoesNotThrow(
                () ->
                        AbstractSourceExecuteProcessor
                                .validateSchemaEvolutionIncrementalParallelism(
                                        ConfigFactory.empty()));
    }

    @Test
    void testSchemaEvolutionAcceptsOneIncrementalReader() {
        Config config = ConfigFactory.parseString("incremental.parallelism = 1");

        assertDoesNotThrow(
                () ->
                        AbstractSourceExecuteProcessor
                                .validateSchemaEvolutionIncrementalParallelism(config));
    }

    @Test
    void testSchemaEvolutionRejectsMultipleIncrementalReaders() {
        Config config = ConfigFactory.parseString("incremental.parallelism = 2");

        IllegalArgumentException exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                AbstractSourceExecuteProcessor
                                        .validateSchemaEvolutionIncrementalParallelism(config));
        assertEquals(
                "Flink CDC schema evolution requires incremental.parallelism = 1, but was 2. "
                        + "Multiple incremental readers are not supported by the Flink "
                        + "schema-evolution protocol.",
                exception.getMessage());
    }
}
