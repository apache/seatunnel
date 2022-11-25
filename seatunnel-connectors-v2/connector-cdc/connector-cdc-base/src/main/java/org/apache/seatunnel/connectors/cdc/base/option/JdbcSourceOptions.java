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

package org.apache.seatunnel.connectors.cdc.base.option;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.connectors.cdc.base.source.IncrementalSource;

import java.time.Duration;

/** Configurations for {@link IncrementalSource} of JDBC data source. */
@SuppressWarnings("checkstyle:MagicNumber")
public class JdbcSourceOptions extends SourceOptions {

    public static final Option<String> HOSTNAME =
            Options.key("hostname")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("IP address or hostname of the database server.");

    public static final Option<Integer> PORT =
            Options.key("port")
                    .intType()
                    .defaultValue(3306)
                    .withDescription("Integer port number of the database server.");

    public static final Option<String> USERNAME =
            Options.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Name of the database to use when connecting to the database server.");

    public static final Option<String> PASSWORD =
            Options.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Password to use when connecting to the database server.");

    public static final Option<String> DATABASE_NAME =
            Options.key("database-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Database name of the database to monitor.");

    public static final Option<String> TABLE_NAME =
            Options.key("table-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Table name of the database to monitor.");

    public static final Option<String> SERVER_TIME_ZONE =
            Options.key("server-time-zone")
                    .stringType()
                    .defaultValue("UTC")
                    .withDescription("The session time zone in database server.");

    public static final Option<String> SERVER_ID =
            Options.key("server-id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "A numeric ID or a numeric ID range of this database client, "
                                    + "The numeric ID syntax is like '5400', the numeric ID range syntax "
                                    + "is like '5400-5408', The numeric ID range syntax is recommended when "
                                    + "'scan.incremental.snapshot.enabled' enabled. Every ID must be unique across all "
                                    + "currently-running database processes in the MySQL cluster. This connector"
                                    + " joins the MySQL  cluster as another server (with this unique ID) "
                                    + "so it can read the binlog. By default, a random number is generated between"
                                    + " 5400 and 6400, though we recommend setting an explicit value.");

    public static final Option<Duration> CONNECT_TIMEOUT =
            Options.key("connect.timeout")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(30))
                    .withDescription(
                            "The maximum time that the connector should wait after trying to connect to the database server before timing out.");

    public static final Option<Integer> CONNECTION_POOL_SIZE =
            Options.key("connection.pool.size")
                    .intType()
                    .defaultValue(20)
                    .withDescription("The connection pool size.");

    public static final Option<Integer> CONNECT_MAX_RETRIES =
            Options.key("connect.max-retries")
                    .intType()
                    .defaultValue(3)
                    .withDescription(
                            "The max retry times that the connector should retry to build database server connection.");

    public static final Option<Double> CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND =
        Options.key("chunk-key.even-distribution.factor.upper-bound")
            .doubleType()
            .defaultValue(1000.0d)
            .withDescription(
                "The upper bound of chunk key distribution factor. The distribution factor is used to determine whether the"
                    + " table is evenly distribution or not."
                    + " The table chunks would use evenly calculation optimization when the data distribution is even,"
                    + " and the query for splitting would happen when it is uneven."
                    + " The distribution factor could be calculated by (MAX(id) - MIN(id) + 1) / rowCount.");

    public static final Option<Double> CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND =
        Options.key("chunk-key.even-distribution.factor.lower-bound")
            .doubleType()
            .defaultValue(0.05d)
            .withDescription(
                "The lower bound of chunk key distribution factor. The distribution factor is used to determine whether the"
                    + " table is evenly distribution or not."
                    + " The table chunks would use evenly calculation optimization when the data distribution is even,"
                    + " and the query for splitting would happen when it is uneven."
                    + " The distribution factor could be calculated by (MAX(id) - MIN(id) + 1) / rowCount.");
}
