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

package org.apache.seatunnel.api.sink;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.factory.FactoryUtil;

import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * Factory for creating user-defined dirty data validators.
 *
 * <p>First tries direct class loading, then falls back to SPI discovery via {@link
 * FactoryUtil#discoverFactories}.
 */
@Slf4j
public class DirtyDataValidatorFactory {

    public static DirtyDataValidator createValidator(
            String validatorName, Config config, CatalogTable catalogTable) {
        DirtyDataValidator validator = tryDirectInstantiation(validatorName, config, catalogTable);
        if (validator != null) {
            return validator;
        }

        try {
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            List<DirtyDataValidator> validators =
                    FactoryUtil.discoverFactories(classLoader, DirtyDataValidator.class);

            for (DirtyDataValidator v : validators) {
                if (v.factoryIdentifier().equalsIgnoreCase(validatorName)
                        || v.getClass().getSimpleName().equals(validatorName)
                        || v.getClass().getName().equals(validatorName)) {
                    v.init(config, catalogTable);
                    log.info("Created dirty data validator via SPI: {}", v.getClass().getName());
                    return v;
                }
            }
        } catch (Exception e) {
            log.warn("SPI discovery failed for dirty data validator: {}", validatorName, e);
        }

        log.error("Could not create dirty data validator: {}", validatorName);
        return null;
    }

    private static DirtyDataValidator tryDirectInstantiation(
            String validatorName, Config config, CatalogTable catalogTable) {
        String[] candidates = {
            validatorName, "org.apache.seatunnel.api.sink." + validatorName,
        };
        for (String className : candidates) {
            try {
                Class<?> clazz = Class.forName(className);
                if (DirtyDataValidator.class.isAssignableFrom(clazz)) {
                    DirtyDataValidator validator =
                            (DirtyDataValidator) clazz.getDeclaredConstructor().newInstance();
                    validator.init(config, catalogTable);
                    log.info("Created dirty data validator: {}", className);
                    return validator;
                }
            } catch (Exception ignored) {
            }
        }
        return null;
    }
}
