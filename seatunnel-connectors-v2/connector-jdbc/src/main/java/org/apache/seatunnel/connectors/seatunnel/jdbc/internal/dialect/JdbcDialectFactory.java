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

/**
 * A factory to create a specific {@link JdbcDialect}
 *
 * @see JdbcDialect
 */
public interface JdbcDialectFactory {

    /**
     * Retrieves whether the dialect thinks that it can open a connection to the given URL.
     * Typically, dialects will return <code>true</code> if they understand the sub-protocol
     * specified in the URL and <code>false</code> if they do not.
     *
     * @param url the URL of the database
     * @return <code>true</code> if this dialect understands the given URL; <code>false</code>
     *     otherwise.
     */
    boolean acceptsURL(String url);

    /** @return Creates a new instance of the {@link JdbcDialect}. */
    JdbcDialect create();

    /**
     * Create a {@link JdbcDialect} instance based on the driver type and compatible mode.
     *
     * @param compatibleMode The compatible mode
     * @return a new instance of {@link JdbcDialect}
     */
    default JdbcDialect create(String compatibleMode, String fieldId) {
        return create();
    }
}
