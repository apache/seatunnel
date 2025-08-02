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

package org.apache.seatunnel.transform.nlpmodel.embedding.multimodal;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

@Getter
@AllArgsConstructor
@ToString
public class MultimodalField {

    private String fieldName;
    private ModalityType modalityType;

    public MultimodalField(String fieldSpec) {
        if (fieldSpec == null || fieldSpec.trim().isEmpty()) {
            throw new IllegalArgumentException("Field specification cannot be null or empty");
        }

        String trimmedSpec = fieldSpec.trim();
        if (trimmedSpec.contains(":")) {
            String[] parts = trimmedSpec.split(":", 2);
            String fieldName = parts[0].trim();

            if (fieldName.isEmpty()) {
                throw new IllegalArgumentException(
                        "Field name cannot be empty in specification: " + fieldSpec);
            }
            this.modalityType = ModalityType.ofName(parts[1]);
            this.fieldName = fieldName;

        } else {
            // No type specified, default to text
            this.modalityType = ModalityType.TEXT;
            this.fieldName = trimmedSpec;
        }
    }
}
