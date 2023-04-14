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

package org.apache.seatunnel.connectors.seatunnel.mongodb.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.time.Duration;

public class MongodbConfig {

    public static final String CONNECTOR_IDENTITY = "Mongodb";

    public static final Option<String> CONNECTION =
            Options.key("connection")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Link string of Mongodb");

    public static final Option<String> DATABASE =
            Options.key("database")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Mongodb's database name");

    public static final Option<String> COLLECTION =
            Options.key("collection")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The collection name of Mongodb");

    public static final Option<String> MATCHQUERY =
            Options.key("matchquery")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Mongodb's query syntax");

    public static final Option<String> SPLIT_KEY =
            Options.key("split.key")
                    .stringType()
                    .defaultValue("_id")
                    .withDescription("The key of Mongodb fragmentation");

    public static final Option<Long> SPLIT_SIZE =
            Options.key("split.size")
                    .longType()
                    .defaultValue(64 * 1024 * 1024L)
                    .withDescription("The size of Mongodb fragment");

    public static final Option<String> PROJECTION =
            Options.key("projection")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Fields projection by Mongodb");

    // --------------------------------------------------------------------
    // --------------------------------------------------------------------

    public static final Option<Integer> SCAN_FETCH_SIZE =
            Options.key("scan.fetch-size")
                    .intType()
                    .defaultValue(2048)
                    .withDescription(
                            "Gives the reader a hint as to the number of documents that should be fetched from the database per round-trip when reading. ");

    public static final Option<Boolean> SCAN_CURSOR_NO_TIMEOUT =
            Options.key("scan.cursor.no-timeout")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "The server normally times out idle cursors after an inactivity"
                                    + " period (10 minutes) to prevent excess memory use. Set this option to true to prevent that."
                                    + " However, if the application takes longer than 30 minutes to process the current batch of documents,"
                                    + " the session is marked as expired and closed.");

    //    public static final Option<PartitionStrategy> SCAN_PARTITION_STRATEGY =
    //            Options.key("scan.partition.strategy")
    //                    .enumType(PartitionStrategy.class)
    //                    .defaultValue(PartitionStrategy.DEFAULT)
    //                    .withDescription(
    //                            "Specifies the partition strategy. Available strategies are
    // single, sample, split-vector, sharded and default."
    //                                    + "The single partition strategy treats the entire
    // collection as a single partition."
    //                                    + "The sample partition strategy samples the collection
    // and generate partitions which is fast but possibly uneven."
    //                                    + "The split-vector partition strategy uses the
    // splitVector command to generate partitions for non-sharded collections which is fast and
    // even. The splitVector permission is required."
    //                                    + "The sharded partition strategy reads config.chunks
    // (MongoDB splits a sharded collection into chunks, and the range of the chunks are stored
    // within the collection) as the partitions directly."
    //                                    + "The sharded partition strategy is only used for sharded
    // collection which is fast and even. Read permission of config database is required."
    //                                    + "The default partition strategy uses sharded strategy
    // for sharded collections otherwise using split vector strategy.");
    //
    //    public static final Option<MemorySize> SCAN_PARTITION_SIZE =
    //            Options.key("scan.partition.size")
    //                    .memoryType()
    //                    .defaultValue(MemorySize.parse("64mb"))
    //                    .withDescription("Specifies the partition memory size.");
    //
    //    public static final Option<Integer> SCAN_PARTITION_SAMPLES =
    //            Options.key("scan.partition.samples")
    //                    .intType()
    //                    .defaultValue(10)
    //                    .withDescription(
    //                            "Specifies the samples count per partition. It only takes effect
    // when the partition strategy is sample. "
    //                                    + "The sample partitioner samples the collection, projects
    // and sorts by the partition fields. "
    //                                    + "Then uses every 'scan.partition.samples' as the value
    // to use to calculate the partition boundaries."
    //                                    + "The total number of samples taken is calculated as:
    // samples per partition * (count of documents / number of documents per partition.");
    //
    //    public static final Option<Duration> LOOKUP_RETRY_INTERVAL =
    //            Options.key("lookup.retry.interval")
    //                    .durationType()
    //                    .defaultValue(Duration.ofMillis(1000L))
    //                    .withDescription(
    //                            "Specifies the retry time interval if lookup records from database
    // failed.");
    //
    //    public static final Option<Integer> BUFFER_FLUSH_MAX_ROWS =
    //            Options.key("sink.buffer-flush.max-rows")
    //                    .intType()
    //                    .defaultValue(1000)
    //                    .withDescription(
    //                            "Specifies the maximum number of buffered rows per batch
    // request.");
    //
    //    public static final Option<Duration> BUFFER_FLUSH_INTERVAL =
    //            Options.key("sink.buffer-flush.interval")
    //                    .durationType()
    //                    .defaultValue(Duration.ofSeconds(1))
    //                    .withDescription("Specifies the batch flush interval.");
    //
    //    public static final Option<DeliveryGuarantee> DELIVERY_GUARANTEE =
    //            Options.key("sink.delivery-guarantee")
    //                    .enumType(DeliveryGuarantee.class)
    //                    .defaultValue(DeliveryGuarantee.AT_LEAST_ONCE)
    //                    .withDescription(
    //                            "Optional delivery guarantee when committing. The exactly-once
    // guarantee is not supported yet.");

    public static final Option<Integer> SINK_MAX_RETRIES =
            Options.key("sink.max-retries")
                    .intType()
                    .defaultValue(3)
                    .withDescription(
                            "Specifies the max retry times if writing records to database failed.");

    public static final Option<Duration> SINK_RETRY_INTERVAL =
            Options.key("sink.retry.interval")
                    .durationType()
                    .defaultValue(Duration.ofMillis(1000L))
                    .withDescription(
                            "Specifies the retry time interval if writing records to database failed.");
}
