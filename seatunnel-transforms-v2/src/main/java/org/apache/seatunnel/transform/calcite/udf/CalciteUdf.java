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

/**
 * SPI for Calcite SQL transform UDFs. Implementations must provide a {@code public static eval}
 * method whose signature determines the SQL function's input/output types; for binary/vector data
 * declare {@code byte[]} and the framework bridges Calcite's {@code ByteString} automatically.
 *
 * <p>Annotate the implementation with {@code @AutoService(CalciteUdf.class)} and ship the jar in
 * {@code ${SEATUNNEL_HOME}/lib/} for SPI discovery.
 */
public interface CalciteUdf extends AutoCloseable {

    /** SQL function name used in queries, e.g. "MASK", "DES_ENCRYPT". */
    String functionName();

    /** Open UDF resources. Called once before first eval. */
    default void open() {}

    @Override
    default void close() throws Exception {}
}
