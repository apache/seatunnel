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
package org.apache.seatunnel.transform.execption;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;

public enum GroovyTransformErrorCode implements SeaTunnelErrorCode {

    // 重复命名
    TRANSFORMER_CONFIGURATION_ERROR("GROOVY_TRANSFORM-01", "Transformer configuration error"),
    TRANSFORMER_ILLEGAL_PARAMETER("GROOVY_TRANSFORM-02", "Transformer parameter illegal"),
    TRANSFORMER_RUN_EXCEPTION("GROOVY_TRANSFORM-03", "Transformer run exception"),
    TRANSFORMER_GROOVY_INIT_EXCEPTION("GROOVY_TRANSFORM-04", "Transformer Groovy init exception");

    private final String code;
    private final String description;

    GroovyTransformErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.description;
    }
}
