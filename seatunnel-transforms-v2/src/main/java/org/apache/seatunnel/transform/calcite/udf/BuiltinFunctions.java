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

package org.apache.seatunnel.transform.calcite.udf;

import org.apache.seatunnel.shade.org.apache.calcite.schema.ScalarFunction;
import org.apache.seatunnel.shade.org.apache.calcite.schema.SchemaPlus;
import org.apache.seatunnel.shade.org.apache.calcite.schema.impl.ScalarFunctionImpl;

import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

/** Discovers {@link CalciteUdf} implementations via {@link ServiceLoader} and registers them. */
@Slf4j
public final class BuiltinFunctions {

    private final List<CalciteUdf> loadedUdfs = new ArrayList<>();

    /**
     * Discovers all {@link CalciteUdf} implementations from the classpath, validates them, and
     * registers their static {@code eval} methods into the given Calcite schema.
     */
    public void discoverAndRegister(SchemaPlus schema) {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        ServiceLoader.load(CalciteUdf.class, cl)
                .forEach(
                        udf -> {
                            String rawName = udf.functionName();
                            if (rawName == null || rawName.isEmpty()) {
                                log.warn(
                                        "Skipping Calcite UDF with null/empty functionName: {}",
                                        udf.getClass().getName());
                                return;
                            }
                            String name = rawName.toUpperCase();
                            boolean opened = false;
                            try {
                                Method evalMethod = findStaticEvalMethod(udf.getClass());
                                ScalarFunction function;
                                if (evalMethod != null
                                        && BinaryAwareScalarFunction.requiresBinaryBridging(
                                                evalMethod)) {
                                    function = new BinaryAwareScalarFunction(evalMethod);
                                } else {
                                    function = ScalarFunctionImpl.create(udf.getClass(), "eval");
                                }
                                if (function == null) {
                                    log.warn(
                                            "No valid static eval method found in Calcite UDF: {}",
                                            name);
                                    return;
                                }
                                udf.open();
                                opened = true;
                                schema.add(name, function);
                                loadedUdfs.add(udf);
                                log.info("Registered Calcite UDF via SPI: {}", name);
                            } catch (Exception e) {
                                log.warn("Failed to register Calcite UDF: {}", name, e);
                                if (opened) {
                                    try {
                                        udf.close();
                                    } catch (Exception ce) {
                                        log.warn("Failed to close Calcite UDF: {}", name, ce);
                                    }
                                }
                            }
                        });
    }

    public void close() {
        for (CalciteUdf udf : loadedUdfs) {
            try {
                udf.close();
            } catch (Exception e) {
                log.warn("Failed to close Calcite UDF: {}", udf.functionName(), e);
            }
        }
        loadedUdfs.clear();
    }

    /** Finds the {@code public static eval} method declared on the UDF class, if any. */
    private static Method findStaticEvalMethod(Class<?> clazz) {
        for (Method method : clazz.getMethods()) {
            if ("eval".equals(method.getName())
                    && Modifier.isStatic(method.getModifiers())
                    && Modifier.isPublic(method.getModifiers())) {
                return method;
            }
        }
        return null;
    }
}
