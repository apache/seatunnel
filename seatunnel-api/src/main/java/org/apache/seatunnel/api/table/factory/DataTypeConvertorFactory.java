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

package org.apache.seatunnel.api.table.factory;

import org.apache.seatunnel.api.table.catalog.DataTypeConvertor;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkNotNull;

public class DataTypeConvertorFactory {

    private final Map<String, DataTypeConvertor<?>> dataTypeConvertorMap = new HashMap<>();

    public DataTypeConvertorFactory() {
        this(Thread.currentThread().getContextClassLoader());
    }

    public DataTypeConvertorFactory(ClassLoader classLoader) {
        ServiceLoader.load(DataTypeConvertor.class, classLoader)
                .forEach(
                        dataTypeConvertor -> {
                            dataTypeConvertorMap.put(
                                    dataTypeConvertor.getIdentity().toUpperCase(),
                                    dataTypeConvertor);
                        });
    }

    public DataTypeConvertor<?> getDataTypeConvertor(String convertorIdentify) {
        checkNotNull(convertorIdentify, "connectorIdentify can not be null");
        if (dataTypeConvertorMap.containsKey(convertorIdentify.toUpperCase())) {
            return dataTypeConvertorMap.get(convertorIdentify.toUpperCase());
        }
        throw new IllegalArgumentException(
                "connectorIdentify " + convertorIdentify + " is not supported");
    }
}
