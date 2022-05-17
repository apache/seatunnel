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

import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;

@SuppressWarnings("PMD.AbstractClassShouldStartWithAbstractNamingRule")
public abstract class Column implements Serializable {

    private static final long serialVersionUID = -1L;

    /**
     * column name.
     */
    protected final String name;

    /**
     * Data type of the column.
     */
    protected final SeaTunnelDataType<?> dataType;

    protected final String comment;

    private Column(String name, SeaTunnelDataType<?> dataType, String comment) {
        this.name = name;
        this.dataType = dataType;
        this.comment = comment;
    }

    /**
     * Creates a regular table column that represents physical data.
     */
    public static PhysicalColumn physical(String name, SeaTunnelDataType<?> dataType) {
        return new PhysicalColumn(name, dataType);
    }

    /**
     * Creates a metadata column from metadata of the given column name or from metadata of the
     * given key (if not null).
     *
     * <p>Allows to specify whether the column is virtual or not.
     */
    public static MetadataColumn metadata(
        String name, SeaTunnelDataType<?> dataType, String metadataKey) {
        return new MetadataColumn(name, dataType, metadataKey);
    }

    /**
     * Add the comment to the column and return the new object.
     */
    public abstract Column withComment(String comment);

    /**
     * Returns whether the given column is a physical column of a table; neither computed nor
     * metadata.
     */
    public abstract boolean isPhysical();

    /**
     * Returns the data type of this column.
     */
    public SeaTunnelDataType<?> getDataType() {
        return this.dataType;
    }

    /**
     * Returns the name of this column.
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the comment of this column.
     */
    public Optional<String> getComment() {
        return Optional.ofNullable(comment);
    }

    /**
     * Returns a copy of the column with a replaced {@link SeaTunnelDataType}.
     */
    public abstract Column copy(SeaTunnelDataType<?> newType);

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Column that = (Column) o;
        return Objects.equals(this.name, that.name)
                && Objects.equals(this.dataType, that.dataType)
                && Objects.equals(this.comment, that.comment);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.name, this.dataType);
    }

    // --------------------------------------------------------------------------------------------
    // Specific kinds of columns
    // --------------------------------------------------------------------------------------------

    /**
     * Representation of a physical column.
     */
    public static final class PhysicalColumn extends Column {

        private PhysicalColumn(String name, SeaTunnelDataType<?> dataType) {
            this(name, dataType, null);
        }

        private PhysicalColumn(String name, SeaTunnelDataType<?> dataType, String comment) {
            super(name, dataType, comment);
        }

        @Override
        public PhysicalColumn withComment(String comment) {
            if (comment == null) {
                return this;
            }
            return new PhysicalColumn(name, dataType, comment);
        }

        @Override
        public boolean isPhysical() {
            return true;
        }

        @Override
        public Column copy(SeaTunnelDataType<?> newDataType) {
            return new PhysicalColumn(name, newDataType, comment);
        }
    }

    /**
     * Representation of a metadata column.
     */
    public static final class MetadataColumn extends Column {

        private final String metadataKey;

        private MetadataColumn(
            String name, SeaTunnelDataType<?> dataType, String metadataKey) {
            this(name, dataType, metadataKey, null);
        }

        private MetadataColumn(
                String name,
                SeaTunnelDataType<?> dataType,
                String metadataKey,
                String comment) {
            super(name, dataType, comment);
            this.metadataKey = metadataKey;
        }

        public Optional<String> getMetadataKey() {
            return Optional.ofNullable(metadataKey);
        }

        @Override
        public MetadataColumn withComment(String comment) {
            if (comment == null) {
                return this;
            }
            return new MetadataColumn(name, dataType, metadataKey, comment);
        }

        @Override
        public boolean isPhysical() {
            return false;
        }

        @Override
        public Column copy(SeaTunnelDataType<?> newDataType) {
            return new MetadataColumn(name, newDataType, metadataKey, comment);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            if (!super.equals(o)) {
                return false;
            }
            MetadataColumn that = (MetadataColumn) o;
            return Objects.equals(metadataKey, that.metadataKey);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), metadataKey);
        }
    }
}
