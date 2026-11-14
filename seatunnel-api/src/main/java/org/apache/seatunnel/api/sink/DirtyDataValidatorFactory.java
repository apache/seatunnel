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

import java.lang.reflect.InvocationTargetException;
import java.util.Locale;

/**
 * Resolves validators using the current invocation's context loader. Only a missing class permits
 * SPI lookup; a present but broken implementation must never fall through to another policy.
 */
public class DirtyDataValidatorFactory {

    /**
     * Initializes exactly one explicitly named validator and releases it if initialization fails.
     * Direct class names retain Java case sensitivity; SPI identifiers use locale-independent case.
     */
    public static DirtyDataValidator createValidator(
            String validatorName, Config config, CatalogTable catalogTable) {
        if (validatorName == null || validatorName.trim().isEmpty()) {
            throw new IllegalArgumentException("dirty.validator type must not be blank.");
        }
        validatorName = validatorName.trim();
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if (classLoader == null) {
            classLoader = DirtyDataValidatorFactory.class.getClassLoader();
        }
        DirtyDataValidator validator = directValidator(validatorName, classLoader);
        if (validator == null && !validatorName.contains(".")) {
            validator =
                    directValidator("org.apache.seatunnel.api.sink." + validatorName, classLoader);
        }
        if (validator == null) {
            String identifier = validatorName.toLowerCase(Locale.ROOT);
            for (DirtyDataValidator candidate :
                    FactoryUtil.discoverFactories(classLoader, DirtyDataValidator.class)) {
                String factoryIdentifier = candidate.factoryIdentifier();
                if ((factoryIdentifier != null
                                && identifier.equals(
                                        factoryIdentifier.trim().toLowerCase(Locale.ROOT)))
                        || candidate.getClass().getSimpleName().equals(validatorName)
                        || candidate.getClass().getName().equals(validatorName)) {
                    if (validator != null) {
                        throw new IllegalArgumentException(
                                "Multiple dirty.validator providers for type '"
                                        + validatorName
                                        + "'.");
                    }
                    validator = candidate;
                }
            }
        }
        if (validator == null) {
            throw new IllegalArgumentException(
                    "Could not resolve dirty.validator type '" + validatorName + "'.");
        }
        try {
            validator.init(config, catalogTable);
            return validator;
        } catch (Throwable failure) {
            try {
                validator.close();
            } catch (Throwable closeFailure) {
                if (closeFailure != failure) {
                    failure.addSuppressed(closeFailure);
                }
            }
            if (failure instanceof Error) {
                throw (Error) failure;
            }
            throw new IllegalArgumentException(
                    "Failed to initialize dirty.validator of type '" + validatorName + "'",
                    failure);
        }
    }

    /**
     * Distinguishes an absent class from a broken class without initializing unrelated classes.
     * Constructor and linkage failures remain visible instead of switching to SPI implementations.
     */
    private static DirtyDataValidator directValidator(String className, ClassLoader classLoader) {
        Class<?> type;
        try {
            type = Class.forName(className, false, classLoader);
        } catch (ClassNotFoundException absent) {
            return null;
        }
        if (!DirtyDataValidator.class.isAssignableFrom(type)) {
            throw new IllegalArgumentException(
                    "dirty.validator class '"
                            + className
                            + "' does not implement DirtyDataValidator.");
        }
        try {
            return (DirtyDataValidator) type.getDeclaredConstructor().newInstance();
        } catch (ReflectiveOperationException failure) {
            Throwable cause =
                    failure instanceof InvocationTargetException ? failure.getCause() : failure;
            if (cause instanceof Error) {
                throw (Error) cause;
            }
            throw new IllegalArgumentException(
                    "Could not construct dirty.validator class '" + className + "'.", cause);
        }
    }
}
