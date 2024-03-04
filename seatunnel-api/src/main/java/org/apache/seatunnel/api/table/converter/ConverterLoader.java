/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.api.table.converter;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

public class ConverterLoader {

    public static DataTypeConverter<?> loadDataTypeConverter(String identifier) {
        return loadDataTypeConverter(identifier, Thread.currentThread().getContextClassLoader());
    }

    public static DataTypeConverter<?> loadDataTypeConverter(
            String identifier, ClassLoader classLoader) {
        List<DataTypeConverter> converters =
                discoverConverters(DataTypeConverter.class, classLoader);
        for (DataTypeConverter dataTypeConverter : converters) {
            if (dataTypeConverter.identifier().equals(identifier)) {
                return dataTypeConverter;
            }
        }
        throw new IllegalArgumentException(
                "No data type converter found for identifier: " + identifier);
    }

    public static DataConverter<?> loadDataConverter(String identifier) {
        return loadDataConverter(identifier, Thread.currentThread().getContextClassLoader());
    }

    public static DataConverter<?> loadDataConverter(String identifier, ClassLoader classLoader) {
        List<DataConverter> converters = discoverConverters(DataConverter.class, classLoader);
        for (DataConverter dataConverter : converters) {
            if (dataConverter.identifier().equals(identifier)) {
                return dataConverter;
            }
        }
        throw new IllegalArgumentException("No data converter found for identifier: " + identifier);
    }

    public static TypeConverter<?> loadTypeConverter(String identifier) {
        return loadTypeConverter(identifier, Thread.currentThread().getContextClassLoader());
    }

    public static TypeConverter<?> loadTypeConverter(String identifier, ClassLoader classLoader) {
        List<TypeConverter> converters = discoverConverters(TypeConverter.class, classLoader);
        for (TypeConverter typeConverter : converters) {
            if (typeConverter.identifier().equals(identifier)) {
                return typeConverter;
            }
        }
        throw new IllegalArgumentException("No type converter found for identifier: " + identifier);
    }

    private static <T> List<T> discoverConverters(Class<T> clazz, ClassLoader classLoader) {
        List<T> converters = new ArrayList<>();
        ServiceLoader.load(clazz, classLoader).forEach(t -> converters.add(t));
        return converters;
    }
}
