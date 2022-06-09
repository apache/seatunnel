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

package org.apache.seatunnel.api.table.type;

import java.util.Collections;
import java.util.List;

public class ListType<T> implements CompositeType<List<T>> {

    private final SeaTunnelDataType<T> elementType;

    public ListType(SeaTunnelDataType<T> elementType) {
        this.elementType = elementType;
    }

    public SeaTunnelDataType<T> getElementType() {
        return elementType;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Class<List<T>> getTypeClass() {
        return (Class<List<T>>) (Class<?>) List.class;
    }

    @Override
    public int hashCode() {
        return elementType.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ListType) {
            ListType<?> other = (ListType<?>) obj;
            return elementType.equals(other.elementType);
        } else {
            return false;
        }
    }

    @Override
    public List<SeaTunnelDataType<?>> getChildren() {
        return Collections.singletonList(this.elementType);
    }
}
