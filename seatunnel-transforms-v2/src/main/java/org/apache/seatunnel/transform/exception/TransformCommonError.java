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

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.common.exception.CommonError;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.seatunnel.transform.exception.TransformCommonErrorCode.GET_CATALOG_TABLES_WITH_NOT_EXIST_FIELDS_AND_TABLES_ERROR;
import static org.apache.seatunnel.transform.exception.TransformCommonErrorCode.GET_CATALOG_TABLE_WITH_NOT_EXIST_FIELDS_ERROR;
import static org.apache.seatunnel.transform.exception.TransformCommonErrorCode.GET_CATALOG_TABLE_WITH_NOT_EXIST_TABLES_ERROR;
import static org.apache.seatunnel.transform.exception.TransformCommonErrorCode.INPUT_FIELDS_NOT_FOUND;
import static org.apache.seatunnel.transform.exception.TransformCommonErrorCode.INPUT_FIELD_NOT_FOUND;
import static org.apache.seatunnel.transform.exception.TransformCommonErrorCode.INPUT_TABLE_FIELD_NOT_FOUND;
import static org.apache.seatunnel.transform.exception.TransformCommonErrorCode.INPUT_TABLE_NOT_FOUND;

/** The common error of SeaTunnel transform. Please refer {@link CommonError} */
public class TransformCommonError {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static TransformException cannotFindInputFieldError(String transform, String field) {
        Map<String, String> params = new HashMap<>();
        params.put("field", field);
        params.put("transform", transform);
        return new TransformException(INPUT_FIELD_NOT_FOUND, params);
    }

    public static TransformException cannotFindInputFieldsError(
            String transform, List<String> fields) {
        Map<String, String> params = new HashMap<>();
        params.put("fields", String.join(",", fields));
        params.put("transform", transform);
        return new TransformException(INPUT_FIELDS_NOT_FOUND, params);
    }

    public static TransformException cannotFindInputTableFieldError(
            String transform, String table, String field) {
        Map<String, String> params = new HashMap<>();
        params.put("table", table);
        params.put("field", field);
        params.put("transform", transform);
        return new TransformException(INPUT_TABLE_FIELD_NOT_FOUND, params);
    }

    public static TransformException cannotFindInputTableError(String transform, String table) {
        Map<String, String> params = new HashMap<>();
        params.put("table", table);
        params.put("transform", transform);
        return new TransformException(INPUT_TABLE_NOT_FOUND, params);
    }

    public static TransformException getCatalogTableWithNotExistFields(
            String transform, Map<String, List<String>> fields) {
        Map<String, String> params = new HashMap<>();
        params.put("transform", transform);
        try {
            params.put("tableNotExistedFields", OBJECT_MAPPER.writeValueAsString(fields));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return new TransformException(GET_CATALOG_TABLE_WITH_NOT_EXIST_FIELDS_ERROR, params);
    }

    public static TransformException getCatalogTableWithNotExistTables(
            String transform, List<String> tables) {
        Map<String, String> params = new HashMap<>();
        params.put("transform", transform);
        try {
            params.put("tables", OBJECT_MAPPER.writeValueAsString(tables));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return new TransformException(GET_CATALOG_TABLE_WITH_NOT_EXIST_TABLES_ERROR, params);
    }

    public static TransformException getCatalogTableWithNotExistFieldsAndTables(
            String transform, List<String> tables, Map<String, List<String>> fields) {
        Map<String, String> params = new HashMap<>();
        params.put("transform", transform);
        try {
            params.put("tableNotExistedFields", OBJECT_MAPPER.writeValueAsString(fields));
            params.put("tables", OBJECT_MAPPER.writeValueAsString(tables));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return new TransformException(
                GET_CATALOG_TABLES_WITH_NOT_EXIST_FIELDS_AND_TABLES_ERROR, params);
    }
}
