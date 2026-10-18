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

package org.apache.seatunnel.translation.spark.exception;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;

/** Error codes of the Spark translation layer. */
public enum SparkTranslationErrorCode implements SeaTunnelErrorCode {
    WIDE_DECIMAL_IN_COLLECTION(
            "SPARK-TRANSLATION-01",
            "DECIMAL with precision above 38 inside an ARRAY or MAP is not supported on Spark"),
    INVALID_WIDE_DECIMAL_VALUE(
            "SPARK-TRANSLATION-02",
            "Value does not match the declared DECIMAL type with precision above 38"),
    INVALID_WIDE_DECIMAL_METADATA(
            "SPARK-TRANSLATION-03", "Invalid SeaTunnel decimal metadata on a Spark schema field");

    private final String code;
    private final String description;

    SparkTranslationErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return code;
    }

    @Override
    public String getDescription() {
        return description;
    }
}
