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
 * Decodes a binary value into a string using one of the six documented charsets.
 *
 * <p>Usage: {@code DECODE(binary, charset)}
 */
@AutoService(CalciteUdf.class)
public class DecodeFunction implements CalciteUdf {

    @Override
    public String functionName() {
        return "DECODE";
    }

    public static String eval(byte[] value, String charset) {
        if (value == null || charset == null) {
            return null;
        }
        return StringFunction.decode(value, charset);
    }
}
