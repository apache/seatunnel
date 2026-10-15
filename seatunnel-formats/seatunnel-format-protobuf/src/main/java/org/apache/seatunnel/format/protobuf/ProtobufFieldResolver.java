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

import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Resolves the field names of a SeaTunnel schema against protobuf descriptors.
 *
 * <p>The schema field names and the proto field names are declared independently, so they are not
 * guaranteed to use the same case, for example a schema declaring {@code C_STRING} for {@code
 * string c_string = 6;}. Both converters resolve every field through this class so that reading and
 * writing behave the same way.
 *
 * <p>One instance belongs to one converter and keeps one resolution plan per message descriptor it
 * has seen, because the converters resolve the fields of every record and every column. A plan is
 * therefore bounded by the message types of the proto file the job is configured with, while the
 * per-record cost of resolving a field stays a hash lookup. The instance is not static on purpose:
 * a shared cache would have to keep the descriptors of every job alive for the whole JVM.
 */
final class ProtobufFieldResolver {

    /** Resolution plans, keyed by the message descriptor that owns the resolved fields. */
    private final Map<Descriptors.Descriptor, DescriptorFields> fieldsByDescriptor =
            new ConcurrentHashMap<>();

    private ProtobufFieldResolver() {}

    /** Creates a resolver that reuses one resolution plan per owning message descriptor. */
    static ProtobufFieldResolver create() {
        return new ProtobufFieldResolver();
    }

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
    Descriptors.FieldDescriptor findField(Descriptors.Descriptor descriptor, String fieldName) {
        if (descriptor == null || fieldName == null) {
            return null;
        }
        return fieldsOf(descriptor).findField(fieldName);
    }

    /** Visible for testing: the number of message descriptors with a resolution plan. */
    int cachedDescriptorCount() {
        return fieldsByDescriptor.size();
    }

    /** Returns the resolution plan of one message descriptor, creating it at most once. */
    private DescriptorFields fieldsOf(Descriptors.Descriptor descriptor) {
        return fieldsByDescriptor.computeIfAbsent(descriptor, DescriptorFields::new);
    }

    /** The reusable resolution plan of one message descriptor. */
    private static final class DescriptorFields {

        private final Descriptors.Descriptor descriptor;

        /**
         * Index of the declared fields by their {@link Locale#ROOT} lower case name. It is built
         * lazily when a field name has no exact match, so a job whose schema names match the proto
         * names exactly never pays for it.
         */
        private volatile Map<String, Descriptors.FieldDescriptor> caseInsensitiveIndex;

        private DescriptorFields(Descriptors.Descriptor descriptor) {
            this.descriptor = descriptor;
        }

        private Descriptors.FieldDescriptor findField(String fieldName) {
            Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName(fieldName);
            if (fieldDescriptor != null) {
                return fieldDescriptor;
            }
            return caseInsensitiveIndex().get(fieldName.toLowerCase(Locale.ROOT));
        }

        private Map<String, Descriptors.FieldDescriptor> caseInsensitiveIndex() {
            Map<String, Descriptors.FieldDescriptor> index = caseInsensitiveIndex;
            if (index == null) {
                index = buildCaseInsensitiveIndex();
                caseInsensitiveIndex = index;
            }
            return index;
        }

        private Map<String, Descriptors.FieldDescriptor> buildCaseInsensitiveIndex() {
            Map<String, Descriptors.FieldDescriptor> index = new HashMap<>();
            for (Descriptors.FieldDescriptor candidate : descriptor.getFields()) {
                // A message may declare fields that differ only by case. The fallback then keeps
                // the first declared field, which is the field a scan of getFields() returns.
                index.putIfAbsent(candidate.getName().toLowerCase(Locale.ROOT), candidate);
            }
            return Collections.unmodifiableMap(index);
        }
    }
}
