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

package org.apache.seatunnel.connectors.seatunnel.timeplus.sink.client.executor;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.timeplus.sink.inject.ArrayInjectFunction;
import org.apache.seatunnel.connectors.seatunnel.timeplus.sink.inject.BigDecimalInjectFunction;
import org.apache.seatunnel.connectors.seatunnel.timeplus.sink.inject.DateInjectFunction;
import org.apache.seatunnel.connectors.seatunnel.timeplus.sink.inject.DateTimeInjectFunction;
import org.apache.seatunnel.connectors.seatunnel.timeplus.sink.inject.DoubleInjectFunction;
import org.apache.seatunnel.connectors.seatunnel.timeplus.sink.inject.FloatInjectFunction;
import org.apache.seatunnel.connectors.seatunnel.timeplus.sink.inject.IntInjectFunction;
import org.apache.seatunnel.connectors.seatunnel.timeplus.sink.inject.LongInjectFunction;
import org.apache.seatunnel.connectors.seatunnel.timeplus.sink.inject.MapInjectFunction;
import org.apache.seatunnel.connectors.seatunnel.timeplus.sink.inject.ProtonFieldInjectFunction;
import org.apache.seatunnel.connectors.seatunnel.timeplus.sink.inject.StringInjectFunction;

import lombok.NonNull;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

// import static org.icecream.IceCream.ic;

public class JdbcRowConverter implements Serializable {
    private static final Pattern NULLABLE = Pattern.compile("nullable\\((.*)\\)");
    private static final Pattern LOW_CARDINALITY = Pattern.compile("low_cardinality\\((.*)\\)");
    private static final ProtonFieldInjectFunction DEFAULT_INJECT_FUNCTION =
            new StringInjectFunction();

    private final String[] projectionFields;
    private final Map<String, ProtonFieldInjectFunction> fieldInjectFunctionMap;
    private final Map<String, Function<SeaTunnelRow, Object>> fieldGetterMap;

    public JdbcRowConverter(
            @NonNull SeaTunnelRowType rowType,
            @NonNull Map<String, String> timeplusTableSchema,
            @NonNull String[] projectionFields) {
        this.projectionFields = projectionFields;
        this.fieldInjectFunctionMap =
                createFieldInjectFunctionMap(projectionFields, timeplusTableSchema);
        this.fieldGetterMap = createFieldGetterMap(projectionFields, rowType);
    }

    public PreparedStatement toExternal(SeaTunnelRow row, PreparedStatement statement)
            throws SQLException {
        for (int i = 0; i < projectionFields.length; i++) {
            String fieldName = projectionFields[i];
            Object fieldValue = fieldGetterMap.get(fieldName).apply(row);
            if (fieldValue == null) {
                // field does not exist in row
                // todo: do we need to transform to default value of each type
                statement.setObject(i + 1, null);
                continue;
            }
            ProtonFieldInjectFunction injector =
                    fieldInjectFunctionMap.getOrDefault(fieldName, DEFAULT_INJECT_FUNCTION);
            // ic(fieldName,fieldValue,injector);
            injector.injectFields(statement, i + 1, fieldValue);
        }
        return statement;
    }

    private Map<String, ProtonFieldInjectFunction> createFieldInjectFunctionMap(
            String[] fields, Map<String, String> timeplusTableSchema) {
        Map<String, ProtonFieldInjectFunction> fieldInjectFunctionMap = new HashMap<>();
        for (String field : fields) {
            String fieldType = timeplusTableSchema.get(field);
            ProtonFieldInjectFunction injectFunction =
                    Arrays.asList(
                                    new ArrayInjectFunction(),
                                    new MapInjectFunction(),
                                    new BigDecimalInjectFunction(),
                                    new DateInjectFunction(),
                                    new DateTimeInjectFunction(),
                                    new LongInjectFunction(),
                                    new DoubleInjectFunction(),
                                    new FloatInjectFunction(),
                                    new IntInjectFunction(),
                                    new StringInjectFunction())
                            .stream()
                            .filter(f -> f.isCurrentFieldType(unwrapCommonPrefix(fieldType)))
                            .findFirst()
                            .orElse(new StringInjectFunction());
            fieldInjectFunctionMap.put(field, injectFunction);
        }
        return fieldInjectFunctionMap;
    }

    private Map<String, Function<SeaTunnelRow, Object>> createFieldGetterMap(
            String[] fields, SeaTunnelRowType rowType) {
        Map<String, Function<SeaTunnelRow, Object>> fieldGetterMap = new HashMap<>();
        for (int i = 0; i < fields.length; i++) {
            String fieldName = fields[i];
            int fieldIndex = rowType.indexOf(fieldName);
            fieldGetterMap.put(fieldName, row -> row.getField(fieldIndex));
        }
        return fieldGetterMap;
    }

    private String unwrapCommonPrefix(String fieldType) {
        Matcher nullMatcher = NULLABLE.matcher(fieldType);
        Matcher lowMatcher = LOW_CARDINALITY.matcher(fieldType);
        if (nullMatcher.matches()) {
            return nullMatcher.group(1);
        } else if (lowMatcher.matches()) {
            return lowMatcher.group(1);
        } else {
            return fieldType;
        }
    }
}
