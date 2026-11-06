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

import org.apache.seatunnel.transform.sql.zeta.functions.JsonFunction;

import com.google.auto.service.AutoService;

/**
 * {@code GET_JSON_OBJECT(json, path)} UDF for the Calcite SQL transform.
 *
 * <p>Delegates to {@link JsonFunction#getJsonObject(String, String)} so the Zeta and Calcite
 * engines share one implementation of the path and return semantics.
 *
 * <p>Usage: {@code GET_JSON_OBJECT(json, '$.field.subfield[0]')}
 */
@AutoService(CalciteUdf.class)
public class GetJsonObjectFunction implements CalciteUdf {

    @Override
    public String functionName() {
        return "GET_JSON_OBJECT";
    }

    public static String eval(String json, String path) {
        if (json == null || path == null) {
            return null;
        }
        return JsonFunction.getJsonObject(json, path);
    }
}
