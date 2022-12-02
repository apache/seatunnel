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

package org.apache.seatunnel.api.configuration;

import static com.google.common.base.Preconditions.checkArgument;

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Type;
import java.time.Duration;
import java.util.List;
import java.util.Map;

public class Options {

    /**
     * Starts building a new {@link Option}.
     *
     * @param key The key for the config option.
     * @return The builder for the config option with the given key.
     */
    public static OptionBuilder key(String key) {
        checkArgument(StringUtils.isNotBlank(key), "Option's key not be null.");
        return new OptionBuilder(key);
    }

    /**
     * The option builder is used to create a {@link Option}. It is instantiated via {@link
     * Options#key(String)}.
     */
    public static final class OptionBuilder {
        private final String key;

        /**
         * Creates a new OptionBuilder.
         *
         * @param key The key for the config option
         */
        OptionBuilder(String key) {
            this.key = key;
        }

        /**
         * Defines that the value of the option should be of {@link Boolean} type.
         */
        public TypedOptionBuilder<Boolean> booleanType() {
            return new TypedOptionBuilder<>(key, new TypeReference<Boolean>() {
            });
        }

        /**
         * Defines that the value of the option should be of {@link Integer} type.
         */
        public TypedOptionBuilder<Integer> intType() {
            return new TypedOptionBuilder<>(key, new TypeReference<Integer>() {
            });
        }

        /**
         * Defines that the value of the option should be of {@link Long} type.
         */
        public TypedOptionBuilder<Long> longType() {
            return new TypedOptionBuilder<>(key, new TypeReference<Long>() {
            });
        }

        /**
         * Defines that the value of the option should be of {@link Float} type.
         */
        public TypedOptionBuilder<Float> floatType() {
            return new TypedOptionBuilder<>(key, new TypeReference<Float>() {
            });
        }

        /**
         * Defines that the value of the option should be of {@link Double} type.
         */
        public TypedOptionBuilder<Double> doubleType() {
            return new TypedOptionBuilder<>(key, new TypeReference<Double>() {
            });
        }

        /**
         * Defines that the value of the option should be of {@link String} type.
         */
        public TypedOptionBuilder<String> stringType() {
            return new TypedOptionBuilder<>(key, new TypeReference<String>() {
            });
        }

        /**
         * Defines that the value of the option should be of {@link Duration} type.
         */
        public TypedOptionBuilder<Duration> durationType() {
            return new TypedOptionBuilder<>(key, new TypeReference<Duration>() {
            });
        }

        /**
         * Defines that the value of the option should be of {@link Enum} type.
         *
         * @param enumClass Concrete type of the expected enum.
         */
        public <T extends Enum<T>> TypedOptionBuilder<T> enumType(Class<T> enumClass) {
            return new TypedOptionBuilder<>(key, new TypeReference<T>() {
                @Override
                public Type getType() {
                    return enumClass;
                }
            });
        }

        /**
         * Defines that the value of the option should be a set of properties, which can be
         * represented as {@code Map<String, String>}.
         */
        public TypedOptionBuilder<Map<String, String>> mapType() {
            return new TypedOptionBuilder<>(key, new TypeReference<Map<String, String>>() {
            });
        }

        /**
         * Defines that the value of the option should be a list of properties, which can be
         * represented as {@code List<String>}.
         */
        public TypedOptionBuilder<List<String>> listType() {
            return new TypedOptionBuilder<>(key, new TypeReference<List<String>>() {
            });
        }

        /**
         * Defines that the value of the option should be a list of properties, which can be
         * represented as {@code List<T>}.
         */
        public <T> TypedOptionBuilder<List<T>> listType(Class<T> option) {
            return new TypedOptionBuilder<>(key, new TypeReference<List<T>>() {
            });
        }

        public <T> TypedOptionBuilder<T> objectType(Class<T> option) {
            return new TypedOptionBuilder<>(key, new TypeReference<T>() {
                @Override
                public Type getType() {
                    return option;
                }
            });
        }

        /**
         * The value of the definition option should be represented as T.
         *
         * @param typeReference complex type reference
         */
        public <T> TypedOptionBuilder<T> type(TypeReference<T> typeReference) {
            return new TypedOptionBuilder<>(key, typeReference);
        }
    }

    /**
     * Builder for {@link Option} with a defined atomic type.
     *
     * @param <T> atomic type of the option
     */
    public static class TypedOptionBuilder<T> {
        private final String key;
        private final TypeReference<T> typeReference;

        TypedOptionBuilder(String key, TypeReference<T> typeReference) {
            this.key = key;
            this.typeReference = typeReference;
        }

        /**
         * Creates a Option with the given default value.
         *
         * @param value The default value for the config option
         * @return The config option with the default value.
         */
        public Option<T> defaultValue(T value) {
            return new Option<>(key, typeReference, value);
        }

        /**
         * Creates a Option without a default value.
         *
         * @return The config option without a default value.
         */
        public Option<T> noDefaultValue() {
            return new Option<>(key, typeReference, null);
        }
    }
}
