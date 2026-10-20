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

package org.apache.seatunnel.connectors.seatunnel.python.source;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

/** Verifies factory discovery metadata and the public Python source option contract. */
class PythonSourceFactoryTest {

    @Test
    void testFactoryIdentifier() {
        PythonSourceFactory factory = new PythonSourceFactory();

        Assertions.assertEquals(
                PythonSourceOptions.CONNECTOR_IDENTITY, factory.factoryIdentifier());
        Assertions.assertEquals(PythonSource.class, factory.getSourceClass());
    }

    @Test
    void testOptionRule() {
        PythonSourceFactory factory = new PythonSourceFactory();
        OptionRule rule = factory.optionRule();

        List<Option<?>> requiredOptions =
                rule.getRequiredOptions().stream()
                        .flatMap(requiredOption -> requiredOption.getOptions().stream())
                        .collect(Collectors.toList());
        Assertions.assertTrue(requiredOptions.contains(PythonSourceOptions.PYTHON_SCRIPT_PATH));
        Assertions.assertTrue(requiredOptions.contains(ConnectorCommonOptions.SCHEMA));

        List<Option<?>> optionalOptions = rule.getOptionalOptions();
        Assertions.assertTrue(optionalOptions.contains(PythonSourceOptions.PYTHON_EXECUTABLE));
        Assertions.assertTrue(optionalOptions.contains(PythonSourceOptions.PYTHON_SCRIPT_CONFIG));
        Assertions.assertTrue(
                optionalOptions.contains(PythonSourceOptions.PYTHON_WORKING_DIRECTORY));
        Assertions.assertTrue(optionalOptions.contains(PythonSourceOptions.FILE_FORMAT_TYPE));
        Assertions.assertTrue(optionalOptions.contains(PythonSourceOptions.FIELD_DELIMITER));
    }

    @Test
    void testPublicOptionKeysUseReleasedNames() {
        Assertions.assertEquals("Python", PythonSourceOptions.CONNECTOR_IDENTITY);
        Assertions.assertEquals(
                "python.working.directory", PythonSourceOptions.PYTHON_WORKING_DIRECTORY.key());
    }
}
