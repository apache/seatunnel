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
 * SPI for Calcite SQL transform UDFs. Implementations must provide a <b>public static</b> {@code
 * eval} method whose signature determines the SQL function's input/output types.
 *
 * <p>The {@code eval} method <b>must be static</b> because Calcite's code generation calls it
 * directly without creating an instance. The SPI instance (loaded by {@link
 * java.util.ServiceLoader}) is used only for discovery and lifecycle management.
 *
 * <h3>How to create a custom UDF:</h3>
 *
 * <ol>
 *   <li>Implement this interface and add a <b>public static</b> {@code eval} method:
 *       <pre>{@code
 * @AutoService(CalciteUdf.class)
 * public class MyUdf implements CalciteUdf {
 *     @Override public String functionName() { return "MY_UDF"; }
 *     public static String eval(String input, int length) {
 *         return input.substring(0, length);
 *     }
 * }
 * }</pre>
 *   <li>Package as JAR and place it in {@code ${SEATUNNEL_HOME}/lib/}
 * </ol>
 */
public interface CalciteUdf extends AutoCloseable {

    /** SQL function name used in queries, e.g. "MASK", "DES_ENCRYPT". */
    String functionName();

    /** Open UDF resources. Called once before first eval. */
    default void open() {}

    @Override
    default void close() throws Exception {}
}
