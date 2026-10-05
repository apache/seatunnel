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

import org.apache.seatunnel.transform.sql.zeta.functions.udf.DESUtil;

import com.google.auto.service.AutoService;

/**
 * DES encryption UDF. Password must be at least 8 characters. Returns Base64-encoded ciphertext.
 *
 * <p>Usage: {@code DES_ENCRYPT(password, data)}
 */
@AutoService(CalciteUdf.class)
public class DesEncryptFunction implements CalciteUdf {

    @Override
    public String functionName() {
        return "DES_ENCRYPT";
    }

    public static String eval(String password, String data) {
        if (password == null || data == null) {
            return null;
        }
        return DESUtil.encrypt(password, data);
    }
}
