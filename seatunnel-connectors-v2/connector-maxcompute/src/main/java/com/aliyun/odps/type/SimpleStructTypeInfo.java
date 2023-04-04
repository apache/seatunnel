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

package com.aliyun.odps.type;

import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.maxcompute.exception.MaxcomputeConnectorException;

import com.aliyun.odps.OdpsType;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SimpleStructTypeInfo implements StructTypeInfo {
    private final List<String> fieldNames;
    private final List<TypeInfo> fieldTypeInfos;

    SimpleStructTypeInfo(List<String> names, List<TypeInfo> typeInfos) {
        this.validateParameters(names, typeInfos);
        this.fieldNames = this.toLowerCase(names);
        this.fieldTypeInfos = new ArrayList(typeInfos);
    }

    private List<String> toLowerCase(List<String> names) {
        List<String> lowerNames = new ArrayList(names.size());
        Iterator var3 = names.iterator();

        while (var3.hasNext()) {
            String name = (String) var3.next();
            lowerNames.add(name.toLowerCase());
        }

        return lowerNames;
    }

    private void validateParameters(List<String> names, List<TypeInfo> typeInfos) {
        if (names != null && typeInfos != null && !names.isEmpty() && !typeInfos.isEmpty()) {
            if (names.size() != typeInfos.size()) {
                throw new MaxcomputeConnectorException(
                        CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                        "The amount of field names must be equal to the amount of field types.");
            }
        } else {
            throw new MaxcomputeConnectorException(
                    CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                    "Invalid name or element type for struct.");
        }
    }

    public String getTypeName() {
        StringBuilder stringBuilder = new StringBuilder(this.getOdpsType().name());
        stringBuilder.append("<");

        for (int i = 0; i < this.fieldNames.size(); ++i) {
            if (i > 0) {
                stringBuilder.append(",");
            }

            stringBuilder.append((String) this.fieldNames.get(i));
            stringBuilder.append(":");
            stringBuilder.append(((TypeInfo) this.fieldTypeInfos.get(i)).getTypeName());
        }

        stringBuilder.append(">");
        return stringBuilder.toString();
    }

    public List<String> getFieldNames() {
        return this.fieldNames;
    }

    public List<TypeInfo> getFieldTypeInfos() {
        return this.fieldTypeInfos;
    }

    public int getFieldCount() {
        return this.fieldNames.size();
    }

    public OdpsType getOdpsType() {
        return OdpsType.STRUCT;
    }

    public String toString() {
        return this.getTypeName();
    }
}
