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

import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;

import org.apache.commons.collections4.map.SingletonMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.seatunnel.transform.exception.TransformCommonErrorCode.ENCRYPTION_FAILED;
import static org.apache.seatunnel.transform.exception.TransformCommonErrorCode.EXPRESSION_EXECUTE_ERROR;
import static org.apache.seatunnel.transform.exception.TransformCommonErrorCode.INPUT_FIELDS_NOT_FOUND;
import static org.apache.seatunnel.transform.exception.TransformCommonErrorCode.INPUT_FIELD_NOT_FOUND;
import static org.apache.seatunnel.transform.exception.TransformCommonErrorCode.INPUT_TABLE_NOT_FOUND;
import static org.apache.seatunnel.transform.exception.TransformCommonErrorCode.METADATA_FIELDS_NOT_FOUND;
import static org.apache.seatunnel.transform.exception.TransformCommonErrorCode.METADATA_MAPPING_FIELD_EXISTS;
import static org.apache.seatunnel.transform.exception.TransformCommonErrorCode.SQL_SCHEMA_CHANGE_INCOMPATIBLE;
import static org.apache.seatunnel.transform.exception.TransformCommonErrorCode.WHERE_STATEMENT_ERROR;

/** The common error of SeaTunnel transform. Please refer {@link CommonError} */
public class TransformCommonError {

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

    public static TransformException cannotFindMetadataFieldError(String transform, String field) {
        Map<String, String> params = new HashMap<>();
        params.put("field", field);
        params.put("transform", transform);
        return new TransformException(METADATA_FIELDS_NOT_FOUND, params);
    }

    public static TransformException metadataMappingFieldExists(String transform, String field) {
        Map<String, String> params = new HashMap<>();
        params.put("field", field);
        params.put("transform", transform);
        return new TransformException(METADATA_MAPPING_FIELD_EXISTS, params);
    }

    public static TransformException cannotFindInputTableError(String transform, String table) {
        Map<String, String> params = new HashMap<>();
        params.put("table", table);
        params.put("transform", transform);
        return new TransformException(INPUT_TABLE_NOT_FOUND, params);
    }

    public static TransformException sqlExpressionError(String expression, Throwable cause) {
        Map<String, String> params = new HashMap<>();
        params.put("expression", expression);
        return new TransformException(EXPRESSION_EXECUTE_ERROR, params, cause);
    }

    public static TransformException sqlWhereStatementError(String wherebody, Throwable cause) {
        Map<String, String> params = new HashMap<>();
        params.put("wherebody", wherebody);
        return new TransformException(WHERE_STATEMENT_ERROR, params, cause);
    }

    public static TransformException validationFailed(String message) {
        Map<String, String> params = new SingletonMap<>("message", message);
        return new TransformException(CommonErrorCode.VALIDATION_FAILED, params);
    }

    public static SeaTunnelRuntimeException encryptionError(String field, Throwable cause) {
        Map<String, String> params = new SingletonMap<>("field", field);
        return new TransformException(ENCRYPTION_FAILED, params, cause);
    }

    /**
     * Raised when a schema change cannot be applied to a SQL transform without breaking its query
     * or the schema contract with the sink.
     *
     * @param query the transform query
     * @param table the table the change targets
     * @param statement the upstream DDL statement, may be null
     * @param reason the fixed reason string describing the incompatibility
     * @return the exception to throw
     */
    public static TransformException sqlSchemaChangeIncompatible(
            String query, String table, String statement, String reason) {
        return new TransformException(
                SQL_SCHEMA_CHANGE_INCOMPATIBLE,
                sqlSchemaChangeParams(query, table, statement, reason));
    }

    /**
     * Same as {@link #sqlSchemaChangeIncompatible(String, String, String, String)} with the
     * underlying cause attached.
     */
    public static TransformException sqlSchemaChangeIncompatible(
            String query, String table, String statement, String reason, Throwable cause) {
        return new TransformException(
                SQL_SCHEMA_CHANGE_INCOMPATIBLE,
                sqlSchemaChangeParams(query, table, statement, reason),
                cause);
    }

    private static Map<String, String> sqlSchemaChangeParams(
            String query, String table, String statement, String reason) {
        Map<String, String> params = new HashMap<>();
        params.put("query", String.valueOf(query));
        params.put("table", String.valueOf(table));
        params.put("statement", String.valueOf(statement));
        params.put("reason", String.valueOf(reason));
        return params;
    }
}
