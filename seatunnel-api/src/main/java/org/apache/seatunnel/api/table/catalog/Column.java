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

package org.apache.seatunnel.api.table.catalog;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

import lombok.Data;

import java.io.Serializable;
import java.util.Map;

/**
 * Represent the column of {@link TableSchema}.
 *
 * @see PhysicalColumn
 * @see MetadataColumn
 */
@Data
@SuppressWarnings("PMD.AbstractClassShouldStartWithAbstractNamingRule")
public abstract class Column implements Serializable {

    private static final long serialVersionUID = -1L;

    /** column name. */
    protected final String name;

    /** Data type of the column. */
    // todo: use generic type
    protected final SeaTunnelDataType<?> dataType;

    protected final Integer columnLength;

    /** Does the column can be null */
    protected final boolean nullable;

    // todo: use generic type
    /** The default value of the column. */
    protected final Object defaultValue;

    protected final String comment;

    /** Field type in the database * */
    protected final String sourceType;

    /** Unsigned bit * */
    protected final boolean isUnsigned;

    /** Whether to use the 0 bit * */
    protected final boolean isZeroFill;

    /** Bit length * */
    protected final Long bitLen;

    /** integer may be cross the border * */
    protected final Long longColumnLength;

    /** your options * */
    protected final Map<String, Object> options;

    protected Column(
            String name,
            SeaTunnelDataType<?> dataType,
            Integer columnLength,
            boolean nullable,
            Object defaultValue,
            String comment) {
        this(
                name,
                dataType,
                columnLength,
                nullable,
                defaultValue,
                comment,
                null,
                false,
                false,
                null,
                0L,
                null);
    }

    protected Column(
            String name,
            SeaTunnelDataType<?> dataType,
            Integer columnLength,
            boolean nullable,
            Object defaultValue,
            String comment,
            String sourceType,
            boolean isUnsigned,
            boolean isZeroFill,
            Long bitLen,
            Long longColumnLength,
            Map<String, Object> options) {
        this.name = name;
        this.dataType = dataType;
        this.columnLength = columnLength;
        this.nullable = nullable;
        this.defaultValue = defaultValue;
        this.comment = comment;
        this.sourceType = sourceType;
        this.isUnsigned = isUnsigned;
        this.isZeroFill = isZeroFill;
        this.bitLen = bitLen;
        this.longColumnLength = longColumnLength;
        this.options = options;
    }

    /**
     * Returns whether the given column is a physical column of a table; neither computed nor
     * metadata.
     */
    public abstract boolean isPhysical();

    /** Returns a copy of the column with a replaced {@link SeaTunnelDataType}. */
    public abstract Column copy(SeaTunnelDataType<?> newType);

    /** Returns a copy of the column. */
    public abstract Column copy();
}
