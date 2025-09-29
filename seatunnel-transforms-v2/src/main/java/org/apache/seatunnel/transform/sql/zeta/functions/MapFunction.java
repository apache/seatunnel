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

package org.apache.seatunnel.transform.sql.zeta.functions;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonError;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class MapFunction {
    private MapFunction() {}

    public static Map<String, Object> map(List<Object> args) {
        if (args == null || args.isEmpty()) {
            return new LinkedHashMap<>();
        }
        if (args.size() % 2 != 0) {
            throw CommonError.illegalArgument(
                    args.toString(), "MAP requires even number of arguments");
        }
        Map<String, Object> result = new LinkedHashMap<>(args.size() / 2);
        for (int i = 0; i < args.size(); i += 2) {
            Object keyObj = args.get(i);
            Object val = args.get(i + 1);
            if (keyObj == null) {
                throw CommonError.illegalArgument(args.toString(), "MAP key cannot be null");
            }
            String key = (keyObj instanceof String) ? (String) keyObj : String.valueOf(keyObj);
            result.put(key, val);
        }
        return result;
    }

    public static MapType castMapTypeMapping(Function function, SeaTunnelRowType rowType) {
        List<Expression> expressions = CommonFunction.getExpressions(function);
        if (expressions.size() < 2 || (expressions.size() % 2 != 0)) {
            throw CommonError.illegalArgument(
                    String.valueOf(expressions.size()),
                    "MAP requires even number of arguments >= 2");
        }

        SeaTunnelDataType keyType = null;
        SeaTunnelDataType valType = null;
        for (int i = 0; i < expressions.size(); i += 2) {
            SeaTunnelDataType kt =
                    CommonFunction.resolveExpressionType(expressions.get(i), rowType);
            SeaTunnelDataType vt =
                    CommonFunction.resolveExpressionType(expressions.get(i + 1), rowType);
            keyType = CommonFunction.unifyCollectionType(keyType, kt);
            valType = CommonFunction.unifyCollectionType(valType, vt);
        }
        if (keyType == null) keyType = BasicType.STRING_TYPE;
        if (valType == null) valType = BasicType.STRING_TYPE;
        return new MapType<>(keyType, valType);
    }
}
