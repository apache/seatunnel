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

import com.google.auto.service.AutoService;

/**
 * Data masking UDF: replaces characters in the specified range with a mask character.
 *
 * <p>Usage: {@code MASK(value, start, end, maskChar)}
 *
 * <p>Example: {@code MASK('13812345678', 3, 7, '*')} returns {@code '138****5678'}
 */
@AutoService(CalciteUdf.class)
public class MaskFunction implements CalciteUdf {

    @Override
    public String functionName() {
        return "MASK";
    }

    public static String eval(String value, int start, int end, String maskChar) {
        if (value == null) {
            return null;
        }
        if (start < 0 || end > value.length() || start >= end) {
            return value;
        }
        char mask = (maskChar != null && !maskChar.isEmpty()) ? maskChar.charAt(0) : '*';
        char[] chars = value.toCharArray();
        for (int i = start; i < end; i++) {
            chars[i] = mask;
        }
        return new String(chars);
    }
}
