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
package org.apache.seatunnel.transform.jsonpath;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.transform.common.ErrorHandleWay;

import lombok.ToString;

import java.io.Serializable;

@ToString
public class ColumnConfig implements Serializable {
    private final String path;

    private final String srcField;

    private final String destField;

    private final SeaTunnelDataType<?> destType;
    private final ErrorHandleWay errorHandleWay;

    public ColumnConfig(
            String path,
            String srcField,
            String destField,
            SeaTunnelDataType<?> destType,
            ErrorHandleWay errorHandleWay) {
        this.path = path;
        this.srcField = srcField;
        this.destField = destField;
        this.destType = destType;
        this.errorHandleWay = errorHandleWay;
    }

    public String getPath() {
        return path;
    }

    public String getSrcField() {
        return srcField;
    }

    public String getDestField() {
        return destField;
    }

    public SeaTunnelDataType<?> getDestType() {
        return destType;
    }

    public ErrorHandleWay errorHandleWay() {
        return errorHandleWay;
    }
}
