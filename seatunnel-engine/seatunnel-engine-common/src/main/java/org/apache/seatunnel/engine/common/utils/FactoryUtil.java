/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.seatunnel.engine.common.utils;

import org.apache.seatunnel.engine.common.exception.SeaTunnelEngineException;

import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.InvocationTargetException;
import java.util.LinkedList;
import java.util.List;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

@Slf4j
public class FactoryUtil<T> {

    public static <T> T discoverFactory(ClassLoader classLoader, Class<T> factoryClass, String factoryIdentifier) {
        try {
            final List<T> result = new LinkedList<>();
            ServiceLoader.load(factoryClass, classLoader)
                .iterator()
                .forEachRemaining(result::add);

            List<T> foundFactories = result.stream().filter(f -> factoryClass.isAssignableFrom(f.getClass()))
                .filter(t -> {
                    try {
                        return t.getClass().getMethod("factoryIdentifier").invoke(t).equals(factoryIdentifier);
                    } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
                        throw new SeaTunnelEngineException("Failed to call factoryIdentifier method.");
                    }
                })
                .collect(Collectors.toList());

            if (foundFactories.isEmpty()) {
                throw new SeaTunnelEngineException(
                    String.format(
                        "Could not find any factories that implement '%s' in the classpath.",
                        factoryClass.getName()));
            }

            if (foundFactories.size() > 1) {
                throw new SeaTunnelEngineException(
                    String.format(
                        "Multiple factories for identifier '%s' that implement '%s' found in the classpath.\n\n"
                            + "Ambiguous factory classes are:\n\n"
                            + "%s",
                        factoryIdentifier,
                        factoryClass.getName(),
                        foundFactories.stream()
                            .map(f -> f.getClass().getName())
                            .sorted()
                            .collect(Collectors.joining("\n"))));
            }

            return foundFactories.get(0);
        } catch (ServiceConfigurationError e) {
            log.error("Could not load service provider for factories.", e);
            throw new SeaTunnelEngineException("Could not load service provider for factories.", e);
        }
    }

}
