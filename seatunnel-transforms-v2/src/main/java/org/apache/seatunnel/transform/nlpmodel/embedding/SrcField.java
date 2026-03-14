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

package org.apache.seatunnel.transform.nlpmodel.embedding;

import lombok.Data;

import java.io.Serializable;
import java.util.Base64;

@Data
public class SrcField implements Serializable {

    private static final long serialVersionUID = 1L;

    private SrcFieldSpec fieldSpec;

    private Object fieldValue;

    public SrcField(SrcFieldSpec spec, Object value) {
        this.fieldSpec = spec;
        this.fieldValue = value;
    }

    public String toBase64() {
        if (fieldSpec == null || !fieldSpec.isBinary()) {
            throw new IllegalArgumentException("Payload format must be binary");
        }
        if (fieldValue == null) {
            throw new IllegalArgumentException("Binary data cannot be null or empty");
        }
        if (fieldValue instanceof byte[]) {
            return Base64.getEncoder().encodeToString((byte[]) fieldValue);
        } else {
            return Base64.getEncoder().encodeToString(fieldValue.toString().getBytes());
        }
    }
}
