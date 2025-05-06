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
package org.apache.seatunnel.transform.python;


import lombok.Getter;
import lombok.ToString;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.transform.common.ErrorHandleWay;

@ToString
@Getter
public class PythonColumnConfig {

    private final String destField;

    @Getter
    private final Column destColumn;
    private final ErrorHandleWay errorHandleWay;


    public PythonColumnConfig(String destField,
                              Column destColumn,
                              ErrorHandleWay errorHandleWay) {
        this.destField = destField;
        this.destColumn = destColumn;
        this.errorHandleWay = errorHandleWay;
    }

    public SeaTunnelDataType<?> getDestType() {
        return destColumn.getDataType();
    }
}
