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

package org.apache.seatunnel.connectors.seatunnel.paimon.sink.schema;

import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeChecks;
import org.apache.paimon.types.DataTypeRoot;

import java.util.Arrays;
import java.util.List;

public class UpdatedDataFields {
    private static final List<DataTypeRoot> STRING_TYPES =
            Arrays.asList(DataTypeRoot.CHAR, DataTypeRoot.VARCHAR);
    private static final List<DataTypeRoot> BINARY_TYPES =
            Arrays.asList(DataTypeRoot.BINARY, DataTypeRoot.VARBINARY);
    private static final List<DataTypeRoot> INTEGER_TYPES =
            Arrays.asList(
                    DataTypeRoot.TINYINT,
                    DataTypeRoot.SMALLINT,
                    DataTypeRoot.INTEGER,
                    DataTypeRoot.BIGINT);
    private static final List<DataTypeRoot> FLOATING_POINT_TYPES =
            Arrays.asList(DataTypeRoot.FLOAT, DataTypeRoot.DOUBLE);

    private static final List<DataTypeRoot> DECIMAL_TYPES = Arrays.asList(DataTypeRoot.DECIMAL);

    private static final List<DataTypeRoot> TIMESTAMP_TYPES =
            Arrays.asList(DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE);

    private static final List<DataTypeRoot> TIME_TYPES =
            Arrays.asList(DataTypeRoot.TIME_WITHOUT_TIME_ZONE);

    public static ConvertAction canConvert(DataType oldType, DataType newType) {
        if (oldType.equalsIgnoreNullable(newType)) {
            return ConvertAction.CONVERT;
        }

        int oldIdx = STRING_TYPES.indexOf(oldType.getTypeRoot());
        int newIdx = STRING_TYPES.indexOf(newType.getTypeRoot());
        if (oldIdx >= 0 && newIdx >= 0) {
            return DataTypeChecks.getLength(oldType) <= DataTypeChecks.getLength(newType)
                    ? ConvertAction.CONVERT
                    : ConvertAction.IGNORE;
        }

        oldIdx = BINARY_TYPES.indexOf(oldType.getTypeRoot());
        newIdx = BINARY_TYPES.indexOf(newType.getTypeRoot());
        if (oldIdx >= 0 && newIdx >= 0) {
            return DataTypeChecks.getLength(oldType) <= DataTypeChecks.getLength(newType)
                    ? ConvertAction.CONVERT
                    : ConvertAction.IGNORE;
        }

        oldIdx = INTEGER_TYPES.indexOf(oldType.getTypeRoot());
        newIdx = INTEGER_TYPES.indexOf(newType.getTypeRoot());
        if (oldIdx >= 0 && newIdx >= 0) {
            return oldIdx <= newIdx ? ConvertAction.CONVERT : ConvertAction.IGNORE;
        }

        oldIdx = FLOATING_POINT_TYPES.indexOf(oldType.getTypeRoot());
        newIdx = FLOATING_POINT_TYPES.indexOf(newType.getTypeRoot());
        if (oldIdx >= 0 && newIdx >= 0) {
            return oldIdx <= newIdx ? ConvertAction.CONVERT : ConvertAction.IGNORE;
        }

        oldIdx = DECIMAL_TYPES.indexOf(oldType.getTypeRoot());
        newIdx = DECIMAL_TYPES.indexOf(newType.getTypeRoot());
        if (oldIdx >= 0 && newIdx >= 0) {
            return DataTypeChecks.getPrecision(newType) <= DataTypeChecks.getPrecision(oldType)
                            && DataTypeChecks.getScale(newType) <= DataTypeChecks.getScale(oldType)
                    ? ConvertAction.IGNORE
                    : ConvertAction.CONVERT;
        }

        oldIdx = TIMESTAMP_TYPES.indexOf(oldType.getTypeRoot());
        newIdx = TIMESTAMP_TYPES.indexOf(newType.getTypeRoot());
        if (oldIdx >= 0 && newIdx >= 0) {
            return DataTypeChecks.getPrecision(oldType) <= DataTypeChecks.getPrecision(newType)
                    ? ConvertAction.CONVERT
                    : ConvertAction.IGNORE;
        }

        oldIdx = TIME_TYPES.indexOf(oldType.getTypeRoot());
        newIdx = TIME_TYPES.indexOf(newType.getTypeRoot());
        if (oldIdx >= 0 && newIdx >= 0) {
            return DataTypeChecks.getPrecision(oldType) <= DataTypeChecks.getPrecision(newType)
                    ? ConvertAction.CONVERT
                    : ConvertAction.IGNORE;
        }

        return ConvertAction.EXCEPTION;
    }

    /**
     * Return type of {@link UpdatedDataFields#canConvert(DataType, DataType)}. This enum indicates
     * the action to perform.
     */
    public enum ConvertAction {

        /** {@code oldType} can be converted to {@code newType}. */
        CONVERT,

        /**
         * {@code oldType} and {@code newType} belongs to the same type family, but old type has
         * higher precision than new type. Ignore this convert request.
         */
        IGNORE,

        /**
         * {@code oldType} and {@code newType} belongs to different type family. Throw an exception
         * indicating that this convert request cannot be handled.
         */
        EXCEPTION
    }
}
