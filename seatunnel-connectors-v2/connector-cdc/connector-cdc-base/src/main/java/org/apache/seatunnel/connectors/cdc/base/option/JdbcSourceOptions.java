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

import java.time.ZoneId;
import java.util.List;

/** Configurations for {@link IncrementalSource} of JDBC data source. */
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

    public static final Option<List<String>> DATABASE_NAMES =
            Options.key("database-names")
                    .listType()
                    .noDefaultValue()
                    .withDescription("Database name of the database to monitor.");

    public static final Option<String> SERVER_TIME_ZONE =
            Options.key("server-time-zone")
                    .stringType()
                    .defaultValue(ZoneId.systemDefault().getId())
                    .withDescription(
                            "The session time zone in database server."
                                    + "If not set, then ZoneId.systemDefault() is used to determine the server time zone");

    public static final Option<String> SERVER_ID =
            Options.key("server-id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "A numeric ID or a numeric ID range of this database client, "
                                    + "The numeric ID syntax is like '5400', the numeric ID range syntax "
                                    + "is like '5400-5408'. Every ID must be unique across all "
                                    + "currently-running database processes in the MySQL cluster. This connector"
                                    + " joins the MySQL  cluster as another server (with this unique ID) "
                                    + "so it can read the binlog. By default, a random number is generated between"
                                    + " 5400 and 6400, though we recommend setting an explicit value.");

    public static final Option<Long> CONNECT_TIMEOUT_MS =
            Options.key("connect.timeout.ms")
                    .longType()
                    .defaultValue(30000L)
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
                    .defaultValue(100.0d)
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

    public static final Option<Integer> SAMPLE_SHARDING_THRESHOLD =
            Options.key("sample-sharding.threshold")
                    .intType()
                    .defaultValue(1000) // 1000 shards
                    .withDescription(
                            "The threshold of estimated shard count to trigger the sample sharding strategy. "
                                    + "When the distribution factor is outside the upper and lower bounds, "
                                    + "and if the estimated shard count (approximateRowCnt/chunkSize) exceeds this threshold, "
                                    + "the sample sharding strategy will be used. "
                                    + "This strategy can help to handle large datasets more efficiently. "
                                    + "The default value is 1000 shards.");
    public static final Option<Integer> INVERSE_SAMPLING_RATE =
            Options.key("inverse-sampling.rate")
                    .intType()
                    .defaultValue(1000) // 1/1000 sampling rate
                    .withDescription(
                            "The inverse of the sampling rate for the sample sharding strategy. "
                                    + "The value represents the denominator of the sampling rate fraction. "
                                    + "For example, a value of 1000 means a sampling rate of 1/1000. "
                                    + "This parameter is used when the sample sharding strategy is triggered.");
}
