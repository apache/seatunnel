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

package org.apache.seatunnel.format.protobuf;

import com.google.protobuf.Descriptors;

import java.util.Locale;

/**
 * Resolves the field names of a SeaTunnel schema against a protobuf descriptor.
 *
 * <p>The schema field names and the proto field names are declared independently, so they are not
 * guaranteed to use the same case, for example a schema declaring {@code C_STRING} for {@code
 * string c_string = 6;}. Both converters resolve every field through this class so that reading and
 * writing behave the same way.
 */
final class ProtobufFieldResolver {

    private ProtobufFieldResolver() {}

    /**
     * Finds the protobuf field a schema field name refers to.
     *
     * <p>The exact name is tried first, which keeps proto files that declare mixed case field names
     * such as {@code C_INT32} resolvable exactly. Only when the exact name does not exist a
     * case-insensitive lookup is performed. The fallback is locale independent, because {@code
     * String.toLowerCase()} without a locale uses the JVM default locale and would, for example,
     * fail to match under the Turkish locale.
     *
     * @param descriptor descriptor whose fields are searched, may be {@code null}
     * @param fieldName schema field name, may be {@code null}
     * @return the matching field, or {@code null} when the descriptor declares no such field
     */
    static Descriptors.FieldDescriptor findField(
            Descriptors.Descriptor descriptor, String fieldName) {
        if (descriptor == null || fieldName == null) {
            return null;
        }
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName(fieldName);
        if (fieldDescriptor != null) {
            return fieldDescriptor;
        }
        String lowerCaseName = fieldName.toLowerCase(Locale.ROOT);
        for (Descriptors.FieldDescriptor candidate : descriptor.getFields()) {
            if (candidate.getName().toLowerCase(Locale.ROOT).equals(lowerCaseName)) {
                return candidate;
            }
        }
        return null;
    }
}
