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
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SparkRowEncoderTest {

    @Test
    public void createWithRuntimeRowEncoder() {
        Encoder<Row> encoder = SparkRowEncoder.create(testSchema());

        Assertions.assertNotNull(encoder);
    }

    @Test
    public void createWithRowEncoderFallback() {
        Encoder<Row> encoder =
                SparkRowEncoder.create(
                        testSchema(), OldSparkEncoders.class, TestRowEncoder.class.getName());

        Assertions.assertNotNull(encoder);
    }

    private static StructType testSchema() {
        return new StructType().add("id", DataTypes.IntegerType, false);
    }

    public static class OldSparkEncoders {}

    public static class TestRowEncoder {

        public static Encoder<Row> apply(StructType schema) {
            return RowEncoder.apply(schema);
        }
    }
}
