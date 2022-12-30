/**
 * Copyright @ 2022科大讯飞。 All rights reserved.
 *
 * @author: gdliu3
 * @Date: 2022/12/30 14:28
 */

package org.apache.seatunnel.engine.common.utils;

import org.apache.seatunnel.engine.common.exception.SeaTunnelEngineException;

import lombok.extern.slf4j.Slf4j;

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

            return result.get(0);
        } catch (ServiceConfigurationError e) {
            log.error("Could not load service provider for factories.", e);
            throw new SeaTunnelEngineException("Could not load service provider for factories.", e);
        }
    }

}
