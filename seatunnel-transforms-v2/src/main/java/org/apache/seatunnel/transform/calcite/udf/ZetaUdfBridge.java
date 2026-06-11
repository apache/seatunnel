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

import org.apache.seatunnel.shade.org.apache.calcite.schema.SchemaPlus;
import org.apache.seatunnel.shade.org.apache.calcite.schema.impl.ScalarFunctionImpl;

import org.apache.seatunnel.transform.sql.zeta.ZetaUDF;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

/**
 * Bridges existing {@link ZetaUDF} SPI implementations into the Calcite schema. This allows users
 * who already have ZetaUDF jars deployed in {@code ${SEATUNNEL_HOME}/lib} to use them in Calcite
 * Transform without any changes.
 *
 * <p>Each ZetaUDF is wrapped as a Calcite ScalarFunction that delegates to {@link
 * ZetaUDF#evaluate(List)}.
 */
@Slf4j
public final class ZetaUdfBridge {

    private final List<ZetaUDF> loadedUdfs = new ArrayList<>();

    public void loadAndRegister(SchemaPlus schema) {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        ServiceLoader.load(ZetaUDF.class, classLoader).forEach(loadedUdfs::add);

        for (ZetaUDF udf : loadedUdfs) {
            String funcName = udf.functionName().toUpperCase();
            try {
                udf.open();
            } catch (Exception e) {
                log.warn("Failed to open ZetaUDF: {}", funcName, e);
                continue;
            }
            schema.add(funcName, ScalarFunctionImpl.create(udf.getClass(), "evaluate"));
            log.info("Registered ZetaUDF via SPI bridge: {}", funcName);
        }
    }

    public void close() {
        for (ZetaUDF udf : loadedUdfs) {
            try {
                udf.close();
            } catch (Exception e) {
                log.warn("Failed to close ZetaUDF: {}", udf.functionName(), e);
            }
        }
        loadedUdfs.clear();
    }
}
