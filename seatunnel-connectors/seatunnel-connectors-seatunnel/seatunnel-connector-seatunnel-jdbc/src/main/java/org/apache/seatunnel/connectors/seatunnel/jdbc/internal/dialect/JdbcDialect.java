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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect;

import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.JdbcRowConverter;

import java.io.Serializable;

/**
 * Represents a dialect of SQL implemented by a particular JDBC system. Dialects should be immutable
 * and stateless.
 */

public interface JdbcDialect extends Serializable {

    /**
     * Get the name of jdbc dialect.
     *
     * @return the dialect name.
     */
    String dialectName();

    /**
     * Get converter that convert jdbc object to seatunnel internal object.
     *
     * @return a row converter for the database
     */
    JdbcRowConverter getRowConverter();


    /**
     * get jdbc meta-information type to seatunnel data type mapper.
     * @return a type mapper for the database
     */
    JdbcDialectTypeMapper getJdbcDialectTypeMapper();

}
