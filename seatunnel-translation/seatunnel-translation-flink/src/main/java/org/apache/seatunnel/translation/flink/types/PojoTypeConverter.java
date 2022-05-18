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

package org.apache.seatunnel.translation.flink.types;

import org.apache.seatunnel.api.table.type.PojoType;

import org.apache.flink.api.java.typeutils.PojoTypeInfo;

public class PojoTypeConverter<T1> implements FlinkTypeConverter<PojoType<T1>, PojoTypeInfo<T1>> {

    @Override
    public PojoTypeInfo<T1> convert(PojoType<T1> seaTunnelDataType) {
        Class<T1> pojoClass = seaTunnelDataType.getPojoClass();
        return (PojoTypeInfo<T1>) PojoTypeInfo.of(pojoClass);
    }

    @Override
    public PojoType<T1> reconvert(PojoTypeInfo<T1> typeInformation) {
        return new PojoType<>(typeInformation.getTypeClass());
    }
}
