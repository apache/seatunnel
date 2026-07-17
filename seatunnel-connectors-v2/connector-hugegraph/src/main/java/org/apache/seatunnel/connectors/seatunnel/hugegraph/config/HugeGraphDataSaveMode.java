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

package org.apache.seatunnel.connectors.seatunnel.hugegraph.config;

/**
 * Controls how pre-existing data in the target graph is handled before the Sink writes. Mirrors the
 * SeaTunnel standard {@code DataSaveMode} naming but implemented locally within the connector.
 */
public enum HugeGraphDataSaveMode {

    /** Keep existing data; new elements are written on top (default). */
    APPEND_DATA,

    /**
     * Before writing, delete the existing data of <em>only the labels this job's mappings
     * target</em> (edges then vertices), leaving their schema and any other labels' data intact.
     * Deleting a vertex also removes its incident edges on the server. Scoped per label so, with a
     * multi-table sink, dropping one table does not wipe another; and on checkpoint restart the
     * drop is not re-run, so data written before the restart survives.
     */
    DROP_DATA
}
