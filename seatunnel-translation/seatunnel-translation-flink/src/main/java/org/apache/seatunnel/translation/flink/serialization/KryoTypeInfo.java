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

package org.apache.seatunnel.translation.flink.serialization;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;

/**
 * Operator chaining can avoid serialization and deserialization while the job is running. However,
 * the Flink Job initializes to determine if it is a generic type. Disabling serialization of
 * generic types causes an exception, even though the operator does not serialize at run time.
 */
public class KryoTypeInfo<T> extends GenericTypeInfo<T> {
    private static final long serialVersionUID = -4367528355992922603L;

    public KryoTypeInfo(Class<T> typeClass) {
        super(typeClass);
    }

    @Override
    public TypeSerializer<T> createSerializer(ExecutionConfig config) {
        return new KryoSerializer<T>(getTypeClass(), config);
    }
}
