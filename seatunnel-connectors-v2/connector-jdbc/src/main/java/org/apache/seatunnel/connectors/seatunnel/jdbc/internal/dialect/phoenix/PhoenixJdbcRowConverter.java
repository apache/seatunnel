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

<<<<<<<< HEAD:seatunnel-engine/seatunnel-engine-core/src/main/java/org/apache/seatunnel/engine/core/dag/actions/Action.java
package org.apache.seatunnel.engine.core.dag.actions;
========
package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.phoenix;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.AbstractJdbcRowConverter;
>>>>>>>> upstream/dev:seatunnel-connectors-v2/connector-jdbc/src/main/java/org/apache/seatunnel/connectors/seatunnel/jdbc/internal/dialect/phoenix/PhoenixJdbcRowConverter.java

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

<<<<<<<< HEAD:seatunnel-engine/seatunnel-engine-core/src/main/java/org/apache/seatunnel/engine/core/dag/actions/Action.java
import java.io.Serializable;
import java.net.URL;
import java.util.List;
import java.util.Set;

public interface Action extends Serializable {
    @NonNull
    String getName();

    void setName(@NonNull String name);

    @NonNull
    List<Action> getUpstream();

    void addUpstream(@NonNull Action action);

    int getParallelism();

    void setParallelism(int parallelism);

    long getId();

    Set<URL> getJarUrls();
========
public class PhoenixJdbcRowConverter extends AbstractJdbcRowConverter {
    @Override
    public String converterName() {
        return "Phoenix";
    }

    @Override
    public SeaTunnelRow toInternal(ResultSet rs, ResultSetMetaData metaData, SeaTunnelRowType typeInfo) throws SQLException {
        return super.toInternal(rs, metaData, typeInfo);
    }
>>>>>>>> upstream/dev:seatunnel-connectors-v2/connector-jdbc/src/main/java/org/apache/seatunnel/connectors/seatunnel/jdbc/internal/dialect/phoenix/PhoenixJdbcRowConverter.java
}
