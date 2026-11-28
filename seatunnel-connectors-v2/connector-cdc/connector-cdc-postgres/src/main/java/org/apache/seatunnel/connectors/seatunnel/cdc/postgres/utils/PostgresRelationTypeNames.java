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

package org.apache.seatunnel.connectors.seatunnel.cdc.postgres.utils;

import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.exception.PostgresConnectorErrorCode;

import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableEditor;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * pgoutput RELATION messages identify a column type by OID, and Debezium keeps only the bare {@code
 * pg_type.typname} of it, without quotes or schema. A PostgreSQL sink copies that name into {@code
 * ALTER TABLE ... ADD}, which fails for a name that needs quoting ({@code "JobStatus"}, {@code
 * "order"}) or a type whose schema is not on the {@code search_path}. This class replaces the type
 * name of user-defined enum, range and multirange columns with {@code format_type(oid, NULL)}, the
 * spelling PostgreSQL itself uses in DDL and that {@code PostgresCatalog} already uses for the enum
 * columns of the initial {@code CREATE TABLE}. Composite columns are left alone: Debezium reports
 * them as {@code STRUCT}, which the sink type conversion does not accept.
 */
public final class PostgresRelationTypeNames {

    /** PostgreSQL's FirstNormalObjectId: OIDs below it belong to objects created by initdb. */
    static final long FIRST_NORMAL_OBJECT_ID = 16384L;

    /** {@code pg_type.typtype} of enum, multirange and range types. */
    private static final String USER_DEFINED_TYPE_KINDS = "emr";

    private static final String TYPE_NAMES_SQL =
            "SELECT t.oid::int8 AS oid, t.typname, t.typtype,"
                    + " pg_catalog.format_type(t.oid, NULL) AS type_name"
                    + " FROM pg_catalog.pg_type t WHERE t.oid::int8 = ANY (?)";

    private PostgresRelationTypeNames() {}

    /**
     * Returns the relation with the type name of each user-defined column replaced by its DDL
     * spelling. Built-in and extension base types, composites, domains and arrays keep their names.
     * Does not query the database when no column has a type created after initdb.
     *
     * @throws SQLException if the type names cannot be read
     * @throws SeaTunnelRuntimeException if a column type created after initdb is not found
     */
    public static Table qualify(Connection connection, Table relation) throws SQLException {
        Set<Long> oids = candidateOids(relation);
        if (oids.isEmpty()) {
            return relation;
        }
        return qualify(relation, readTypes(connection, oids));
    }

    /**
     * Rewrites eligible type names using the supplied OID lookup while preserving Debezium's type
     * expression. Every candidate OID must be present in the map.
     *
     * @throws SeaTunnelRuntimeException if a candidate OID is missing from the lookup
     */
    static Table qualify(Table relation, Map<Long, PgType> types) {
        TableEditor editor = null;
        for (Column column : relation.columns()) {
            long oid = Integer.toUnsignedLong(column.nativeType());
            if (oid < FIRST_NORMAL_OBJECT_ID) {
                continue;
            }
            PgType type = types.get(oid);
            if (type == null) {
                throw new SeaTunnelRuntimeException(
                        PostgresConnectorErrorCode.RESOLVE_COLUMN_TYPE_NAME_FAILED,
                        String.format(
                                "Type OID %d of column %s in table %s is not in pg_type, so the"
                                        + " type name of the column cannot be resolved",
                                oid, column.name(), relation.id()));
            }
            // A domain column reports the OID of its base type with the domain's own name. A
            // domain named like its base type, in another schema, is not told apart by this.
            if (!type.isUserDefined() || !type.getName().equals(column.typeName())) {
                continue;
            }
            if (editor == null) {
                editor = relation.edit();
            }
            editor.updateColumn(
                    column.edit().type(type.getDdlName(), column.typeExpression()).create());
        }
        return editor == null ? relation : editor.create();
    }

    private static Set<Long> candidateOids(Table relation) {
        Set<Long> oids = new LinkedHashSet<>();
        for (Column column : relation.columns()) {
            long oid = Integer.toUnsignedLong(column.nativeType());
            if (oid >= FIRST_NORMAL_OBJECT_ID) {
                oids.add(oid);
            }
        }
        return oids;
    }

    private static Map<Long, PgType> readTypes(Connection connection, Set<Long> oids)
            throws SQLException {
        Map<Long, PgType> types = new HashMap<>();
        Array oidArray = connection.createArrayOf("int8", oids.toArray(new Long[0]));
        try (PreparedStatement statement = connection.prepareStatement(TYPE_NAMES_SQL)) {
            statement.setArray(1, oidArray);
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    types.put(
                            resultSet.getLong("oid"),
                            new PgType(
                                    resultSet.getString("typname"),
                                    resultSet.getString("typtype"),
                                    resultSet.getString("type_name")));
                }
            }
        } finally {
            oidArray.free();
        }
        return Collections.unmodifiableMap(types);
    }

    /** The {@code pg_type} row of a column type. */
    static final class PgType {
        private final String name;
        private final String kind;
        private final String ddlName;

        PgType(String name, String kind, String ddlName) {
            this.name = name;
            this.kind = kind;
            this.ddlName = ddlName;
        }

        String getName() {
            return name;
        }

        String getDdlName() {
            return ddlName;
        }

        boolean isUserDefined() {
            return kind != null
                    && kind.length() == 1
                    && USER_DEFINED_TYPE_KINDS.indexOf(kind.charAt(0)) >= 0;
        }
    }
}
