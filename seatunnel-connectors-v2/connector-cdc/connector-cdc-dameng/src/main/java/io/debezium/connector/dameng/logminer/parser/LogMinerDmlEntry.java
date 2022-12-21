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

package io.debezium.connector.dameng.logminer.parser;

import io.debezium.connector.dameng.logminer.Operation;

public interface LogMinerDmlEntry {

    /**
     * @return object array that contains the before state, values from WHERE clause.
     */
    Object[] getOldValues();

    /**
     * @return object array that contains the after state, values from an insert's
     * values list or the values in the SET clause of an update statement.
     */
    Object[] getNewValues();

    /**
     * @return LogMiner event operation type
     */
    Operation getOperation();

    /**
     * @return schema name
     */
    String getObjectOwner();

    /**
     * @return table name
     */
    String getObjectName();

    /**
     * Sets table name
     * @param name table name
     */
    LogMinerDmlEntry setObjectName(String name);

    /**
     * Sets schema owner
     * @param name schema owner
     */
    LogMinerDmlEntry setObjectOwner(String name);
}
