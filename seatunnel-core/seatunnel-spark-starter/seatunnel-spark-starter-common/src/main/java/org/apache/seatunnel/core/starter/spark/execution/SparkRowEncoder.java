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

package org.apache.seatunnel.core.starter.spark.execution;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Creates row encoders for the shared Spark 3 starter code.
 *
 * <p>This module is compiled against Spark 3.3 but also runs with Spark 3.5. Resolve the encoder
 * API at runtime, preferring {@code Encoders.row} when available and falling back to {@code
 * RowEncoder.apply} for older Spark 3 runtimes.
 */
final class SparkRowEncoder {

    private static final String ROW_ENCODER_CLASS =
            "org.apache.spark.sql.catalyst.encoders.RowEncoder";

    private SparkRowEncoder() {}

    static Encoder<Row> create(StructType schema) {
        return create(schema, Encoders.class, ROW_ENCODER_CLASS);
    }

    static Encoder<Row> create(
            StructType schema, Class<?> encodersClass, String rowEncoderClassName) {
        try {
            return invoke(encodersClass.getMethod("row", StructType.class), schema);
        } catch (NoSuchMethodException ignored) {
            return createWithRowEncoder(schema, rowEncoderClassName);
        }
    }

    private static Encoder<Row> createWithRowEncoder(
            StructType schema, String rowEncoderClassName) {
        try {
            return invoke(
                    Class.forName(rowEncoderClassName).getMethod("apply", StructType.class),
                    schema);
        } catch (ClassNotFoundException | NoSuchMethodException e) {
            throw new IllegalStateException(
                    "Spark row encoder is not available. "
                            + "Spark 3.5 Encoders.row or Spark 3.x RowEncoder.apply is required.",
                    e);
        }
    }

    @SuppressWarnings("unchecked")
    private static Encoder<Row> invoke(Method method, StructType schema) {
        try {
            return (Encoder<Row>) method.invoke(null, schema);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException("Cannot access Spark row encoder method.", e);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            if (cause instanceof Error) {
                throw (Error) cause;
            }
            throw new IllegalStateException("Spark row encoder method failed.", cause);
        }
    }
}
