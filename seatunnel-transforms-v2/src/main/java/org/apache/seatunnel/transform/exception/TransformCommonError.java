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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.seatunnel.transform.exception.TransformCommonErrorCode.INPUT_FIELDS_NOT_FOUND;
import static org.apache.seatunnel.transform.exception.TransformCommonErrorCode.INPUT_FIELD_NOT_FOUND;
import static org.apache.seatunnel.transform.exception.TransformCommonErrorCode.METADATA_FIELDS_NOT_FOUND;
import static org.apache.seatunnel.transform.exception.TransformCommonErrorCode.METADATA_MAPPING_FIELD_EXISTS;

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
}
