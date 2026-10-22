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

package org.apache.seatunnel.connectors.seatunnel.cdc.opengauss;

import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.config.PostgresIncrementalSourceOptions;

/**
 * Options of the {@link OpengaussIncrementalSource}.
 *
 * <p>The connector reuses the PostgreSQL CDC runtime through the PG base, so it inherits the
 * PostgreSQL CDC options. OpenGauss-specific option surface (for example the startup modes this
 * connector accepts) is owned by {@link OpengaussSourceOptions}.
 */
public class OpengaussIncrementalSourceOptions extends PostgresIncrementalSourceOptions {}
