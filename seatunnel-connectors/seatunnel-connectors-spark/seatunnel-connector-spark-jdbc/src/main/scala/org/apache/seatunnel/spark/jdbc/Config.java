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
package org.apache.seatunnel.spark.jdbc;

/**
 * Jdbc configuration parameters
 */
public final class Config {

    /**
     * Jdbc driver for source or sink
     */
    public static final String DRIVER = "driver";

    /**
     * Jdbc Url for source or sink
     */
    public static final String URL = "url";

    /**
     * Jdbc username for source or sink
     */
    public static final String USERNAME = "username";

    /**
     * Jdbc username for source or sink
     */
    @Deprecated
    public static final String USE = "user";

    /**
     * Jdbc password for source or sink
     */
    public static final String PASSWORD = "password";

    /**
     * Table for sink
     */
    public static final String TABLE = "table";

    /**
     * Use SSL for sink
     */
    public static final String USE_SSL = "useSsl";

    /**
     * dbTable for sink
     */
    public static final String DB_TABLE = "dbTable";

    /**
     * Isolation level for sink
     */
    public static final String ISOLATION_LEVEL = "isolationLevel";

    /**
     * Custom update stmt for sink
     */
    public static final String CUSTOM_UPDATE_STMT = "customUpdateStmt";

    /**
     * Duplicate incs for sink
     */
    public static final String DUPLICATE_INCS = "duplicateIncs";

    /**
     * Show sql for sink
     */
    public static final String SHOW_SQL = "showSql";
}
