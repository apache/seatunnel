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

package org.apache.seatunnel.transform.calcite.adapter;

import org.apache.seatunnel.shade.org.apache.calcite.DataContext;
import org.apache.seatunnel.shade.org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.seatunnel.shade.org.apache.calcite.linq4j.QueryProvider;
import org.apache.seatunnel.shade.org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.seatunnel.shade.org.apache.calcite.schema.SchemaPlus;

import java.util.Locale;
import java.util.TimeZone;

/** Minimal {@link DataContext} for Bindable execution without a full {@code CalciteConnection}. */
public final class SeaTunnelDataContext implements DataContext {

    private final SchemaPlus rootSchema;
    private final RelDataTypeFactory typeFactory;
    private final TimeZone timeZone;
    private final Locale locale;
    private final String user;

    private long currentEpochMillis;

    public SeaTunnelDataContext(SchemaPlus rootSchema, RelDataTypeFactory typeFactory) {
        this(
                rootSchema,
                typeFactory,
                TimeZone.getDefault(),
                Locale.getDefault(),
                System.getProperty("user.name", ""));
    }

    public SeaTunnelDataContext(
            SchemaPlus rootSchema,
            RelDataTypeFactory typeFactory,
            TimeZone timeZone,
            Locale locale,
            String user) {
        this.rootSchema = rootSchema;
        this.typeFactory = typeFactory;
        this.timeZone = timeZone;
        this.locale = locale;
        this.user = user;
        this.currentEpochMillis = System.currentTimeMillis();
    }

    /**
     * Snapshots the current wall-clock time. The engine should invoke this once per row execution
     * so that {@code CURRENT_TIMESTAMP} / {@code LOCAL_TIMESTAMP} / {@code UTC_TIMESTAMP} remain
     * stable for the duration of a single statement evaluation.
     */
    public void refreshNow() {
        this.currentEpochMillis = System.currentTimeMillis();
    }

    @Override
    public SchemaPlus getRootSchema() {
        return rootSchema;
    }

    @Override
    public JavaTypeFactory getTypeFactory() {
        return (JavaTypeFactory) typeFactory;
    }

    @Override
    public QueryProvider getQueryProvider() {
        return null;
    }

    @Override
    public Object get(String name) {
        if (name == null) {
            return null;
        }
        if (DataContext.Variable.CURRENT_TIMESTAMP.camelName.equals(name)
                || DataContext.Variable.LOCAL_TIMESTAMP.camelName.equals(name)
                || DataContext.Variable.UTC_TIMESTAMP.camelName.equals(name)) {
            return currentEpochMillis;
        }
        if (DataContext.Variable.TIME_ZONE.camelName.equals(name)) {
            return timeZone;
        }
        if (DataContext.Variable.LOCALE.camelName.equals(name)) {
            return locale;
        }
        if (DataContext.Variable.USER.camelName.equals(name)
                || DataContext.Variable.SYSTEM_USER.camelName.equals(name)) {
            return user;
        }
        return null;
    }
}
