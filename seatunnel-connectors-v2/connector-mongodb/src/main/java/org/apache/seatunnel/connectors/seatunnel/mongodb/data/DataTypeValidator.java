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

package org.apache.seatunnel.connectors.seatunnel.mongodb.data;

import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.mongodb.exception.MongodbConnectorException;

public class DataTypeValidator {

    public static void validateDataType(SeaTunnelDataType dataType) throws IllegalArgumentException {
        switch (dataType.getSqlType()) {
            case TIME:
                throw new MongodbConnectorException(CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                    "Unsupported data type: " + dataType);
            case MAP:
                MapType mapType = (MapType) dataType;
                if (!SqlType.STRING.equals(mapType.getKeyType().getSqlType())) {
                    throw new MongodbConnectorException(CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                        "Unsupported map key type: " + mapType.getKeyType());
                }
                break;
            case ROW:
                SeaTunnelRowType rowType = (SeaTunnelRowType) dataType;
                for (int i = 0; i < rowType.getTotalFields(); i++) {
                    validateDataType(rowType.getFieldType(i));
                }
                break;
            default:
        }
    }
}
