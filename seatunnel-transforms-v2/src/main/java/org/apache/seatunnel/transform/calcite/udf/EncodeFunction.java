/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more
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

import org.apache.seatunnel.transform.sql.zeta.functions.StringFunction;

import com.google.auto.service.AutoService;

/**
 * Encodes a string into a binary value using one of the six documented charsets.
 *
 * <p>Usage: {@code ENCODE(string, charset)}
 */
@AutoService(CalciteUdf.class)
public class EncodeFunction implements CalciteUdf {

    @Override
    public String functionName() {
        return "ENCODE";
    }

    public static byte[] eval(String value, String charset) {
        if (value == null || charset == null) {
            return null;
        }
        return StringFunction.encode(value, charset);
    }
}
