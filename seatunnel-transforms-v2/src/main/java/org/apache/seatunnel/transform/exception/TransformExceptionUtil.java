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

package org.apache.seatunnel.transform.exception;

import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

public class TransformExceptionUtil {

    public static <T> void withErrorCheck(
            String transformName, Iterator<T> keys, Consumer<T> execute) {
        Map<String, List<String>> notExistedColumn = new LinkedHashMap<>();
        Set<String> notExistedTable = new LinkedHashSet<>();
        while (keys.hasNext()) {
            try {
                execute.accept(keys.next());
            } catch (SeaTunnelRuntimeException e) {
                if (e.getSeaTunnelErrorCode()
                        .equals(TransformCommonErrorCode.INPUT_TABLE_FIELD_NOT_FOUND)) {
                    String field = e.getParams().get("field");
                    String table = e.getParams().get("table");
                    if (!notExistedColumn.containsKey(table)) {
                        notExistedColumn.put(table, new ArrayList<>());
                    }
                    notExistedColumn.get(table).add(field);
                } else if (e.getSeaTunnelErrorCode()
                        .equals(TransformCommonErrorCode.INPUT_TABLE_NOT_FOUND)) {
                    notExistedTable.add(e.getParams().get("table"));
                } else {
                    throw e;
                }
            }
        }
        if (!notExistedTable.isEmpty() && !notExistedColumn.isEmpty()) {
            throw TransformCommonError.getCatalogTableWithNotExistFieldsAndTables(
                    transformName, new ArrayList<>(notExistedTable), notExistedColumn);
        } else if (!notExistedColumn.isEmpty()) {
            throw TransformCommonError.getCatalogTableWithNotExistFields(
                    transformName, notExistedColumn);
        } else if (!notExistedTable.isEmpty()) {
            throw TransformCommonError.getCatalogTableWithNotExistTables(
                    transformName, new ArrayList<>(notExistedTable));
        }
    }
}
