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

package org.apache.seatunnel.edge.agent.starter.parse;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.FactoryUtil;
import org.apache.seatunnel.edge.agent.connector.EdgeInputReaderFactory;
import org.apache.seatunnel.edge.agent.transport.EdgeCollectorTransportFactory;

import java.util.Objects;

final class EdgeAgentFactoryValidation {

    private EdgeAgentFactoryValidation() {}

    static void validateInput(ClassLoader classLoader, ReadonlyConfig inputConfig, String inputType)
            throws Exception {
        validateWithFactory(classLoader, inputConfig, inputType, EdgeInputReaderFactory.class);
    }

    static void validateOutput(
            ClassLoader classLoader, ReadonlyConfig outputConfig, String outputType)
            throws Exception {
        validateWithFactory(
                classLoader, outputConfig, outputType, EdgeCollectorTransportFactory.class);
    }

    private static <T extends Factory> void validateWithFactory(
            ClassLoader classLoader, ReadonlyConfig config, String type, Class<T> factoryClass)
            throws Exception {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(type, "type");
        T factory = FactoryUtil.discoverFactory(classLoader, factoryClass, type);
        ConfigValidator.of(config).validate(factory.optionRule());
    }
}
