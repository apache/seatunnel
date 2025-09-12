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

package org.apache.seatunnel.api.table.catalog;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/** Represent a physical table schema. */
@EqualsAndHashCode(callSuper = true)
@Data
public final class TableSchema extends AbstractSchema {
    private static final long serialVersionUID = 1L;

    private final PrimaryKey primaryKey;

    private final List<ConstraintKey> constraintKeys;

    public TableSchema(
            List<Column> columns, PrimaryKey primaryKey, List<ConstraintKey> constraintKeys) {
        super(columns);
        this.primaryKey = primaryKey;
        this.constraintKeys = constraintKeys;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private final List<Column> columns = new ArrayList<>();

        private PrimaryKey primaryKey;

        private final List<ConstraintKey> constraintKeys = new ArrayList<>();

        public Builder columns(List<Column> columns) {
            this.columns.addAll(columns);
            return this;
        }

        public Builder column(Column column) {
            this.columns.add(column);
            return this;
        }

        public Builder primaryKey(PrimaryKey primaryKey) {
            this.primaryKey = primaryKey;
            return this;
        }

        public Builder constraintKey(ConstraintKey constraintKey) {
            this.constraintKeys.add(constraintKey);
            return this;
        }

        public Builder constraintKey(List<ConstraintKey> constraintKeys) {
            this.constraintKeys.addAll(constraintKeys);
            return this;
        }

        public TableSchema build() {
            return new TableSchema(columns, primaryKey, constraintKeys);
        }
    }

    public TableSchema copy() {
        List<Column> copyColumns = columns.stream().map(Column::copy).collect(Collectors.toList());
        List<ConstraintKey> copyConstraintKeys =
                constraintKeys.stream().map(ConstraintKey::copy).collect(Collectors.toList());
        return TableSchema.builder()
                .constraintKey(copyConstraintKeys)
                .columns(copyColumns)
                .primaryKey(primaryKey == null ? null : primaryKey.copy())
                .build();
    }

    /**
     * Custom deserialization to handle compatibility with older versions.
     *
     * <p>In older versions (before 2.3.12), TableSchema directly contained columns field. In newer
     * versions, TableSchema extends AbstractSchema which contains columns field.
     *
     * <p>This method ensures that old checkpoint data can be properly deserialized by using
     * ObjectInputStream.GetField to read old version fields and properly initialize the parent
     * class fields.
     */
    private void readObject(ObjectInputStream stream) throws IOException, ClassNotFoundException {
        ObjectInputStream.GetField fields = stream.readFields();

        // Read fields that exist in both old and new versions
        PrimaryKey primaryKey = (PrimaryKey) fields.get("primaryKey", null);
        @SuppressWarnings("unchecked")
        List<ConstraintKey> constraintKeys =
                (List<ConstraintKey>) fields.get("constraintKeys", new ArrayList<>());

        // Try to read the columns field - this will work for old version data
        @SuppressWarnings("unchecked")
        List<Column> columns = null;
        try {
            columns = (List<Column>) fields.get("columns", null);
        } catch (IllegalArgumentException e) {
            // Field doesn't exist in serialized data - this is expected for new version data
            columns = null;
        }

        // If columns is null, it means we're deserializing new version data
        // and the columns field is already properly set in the parent class
        if (columns != null) {
            // This is old version data - we need to initialize the parent class fields
            initializeParentFields(columns);
        }

        // Set the fields in this class using reflection
        setFieldValue("primaryKey", primaryKey);
        setFieldValue("constraintKeys", constraintKeys);
    }

    /** Initialize parent class fields using reflection for old version compatibility. */
    private void initializeParentFields(List<Column> columns) {
        try {
            // Set columns field in parent class
            java.lang.reflect.Field columnsField = AbstractSchema.class.getDeclaredField("columns");
            columnsField.setAccessible(true);
            setFinalField(columnsField, this, columns);

            // Set columnNames field in parent class
            List<String> columnNames =
                    columns.stream().map(Column::getName).collect(Collectors.toList());
            java.lang.reflect.Field columnNamesField =
                    AbstractSchema.class.getDeclaredField("columnNames");
            columnNamesField.setAccessible(true);
            setFinalField(columnNamesField, this, columnNames);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to initialize parent fields for old version compatibility", e);
        }
    }

    /** Set value to a final field using reflection with JDK compatibility. */
    private void setFinalField(java.lang.reflect.Field field, Object target, Object value)
            throws Exception {
        // For JDK 8-11, we can modify the modifiers field
        // For JDK 17+, we use Unsafe or other methods
        try {
            // Try the traditional approach first
            java.lang.reflect.Field modifiersField =
                    java.lang.reflect.Field.class.getDeclaredField("modifiers");
            modifiersField.setAccessible(true);
            int originalModifiers = field.getModifiers();
            modifiersField.setInt(field, originalModifiers & ~java.lang.reflect.Modifier.FINAL);
            field.set(target, value);
            modifiersField.setInt(field, originalModifiers);
        } catch (Exception e) {
            // Fallback for newer JDK versions - use Unsafe
            try {
                Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
                java.lang.reflect.Field unsafeField = unsafeClass.getDeclaredField("theUnsafe");
                unsafeField.setAccessible(true);
                Object unsafe = unsafeField.get(null);

                java.lang.reflect.Method objectFieldOffsetMethod =
                        unsafeClass.getMethod("objectFieldOffset", java.lang.reflect.Field.class);
                long fieldOffset = (long) objectFieldOffsetMethod.invoke(unsafe, field);

                java.lang.reflect.Method putObjectMethod =
                        unsafeClass.getMethod("putObject", Object.class, long.class, Object.class);
                putObjectMethod.invoke(unsafe, target, fieldOffset, value);
            } catch (Exception e2) {
                // Final fallback - direct field access (might not work with final fields)
                field.set(target, value);
            }
        }
    }

    /** Set field value using reflection. */
    private void setFieldValue(String fieldName, Object value) throws RuntimeException {
        try {
            java.lang.reflect.Field field = this.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            setFinalField(field, this, value);
        } catch (Exception e) {
            throw new RuntimeException("Failed to set field " + fieldName, e);
        }
    }
}
