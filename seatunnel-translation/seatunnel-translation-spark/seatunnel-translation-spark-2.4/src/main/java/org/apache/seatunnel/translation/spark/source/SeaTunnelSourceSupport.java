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

package org.apache.seatunnel.translation.spark.source;

import org.apache.seatunnel.api.options.EnvCommonOptions;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.Constants;
import org.apache.seatunnel.common.utils.SerializationUtils;
import org.apache.seatunnel.translation.spark.execution.MultiTableManager;
import org.apache.seatunnel.translation.spark.source.reader.batch.BatchSourceReader;
import org.apache.seatunnel.translation.spark.source.reader.micro.MicroBatchSourceReader;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.MicroBatchReadSupport;
import org.apache.spark.sql.sources.v2.ReadSupport;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.sources.v2.reader.streaming.MicroBatchReader;
import org.apache.spark.sql.types.StructType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class SeaTunnelSourceSupport
        implements DataSourceV2, ReadSupport, MicroBatchReadSupport, DataSourceRegister {
    private static final Logger LOG = LoggerFactory.getLogger(SeaTunnelSourceSupport.class);
    public static final String SEA_TUNNEL_SOURCE_NAME = "SeaTunnelSource";
    public static final Integer CHECKPOINT_INTERVAL_DEFAULT = 10000;

    @Override
    public String shortName() {
        return SEA_TUNNEL_SOURCE_NAME;
    }

    @Override
    public DataSourceReader createReader(StructType rowType, DataSourceOptions options) {
        return createReader(options);
    }

    @Override
    public DataSourceReader createReader(DataSourceOptions options) {
        SeaTunnelSource<SeaTunnelRow, ?, ?> seaTunnelSource = getSeaTunnelSource(options);
        int parallelism = options.getInt(EnvCommonOptions.PARALLELISM.key(), 1);
        Map<String, String> envOptions = options.asMap();
        String applicationId = SparkSession.getActiveSession().get().sparkContext().applicationId();
        List<CatalogTable> catalogTables;
        try {
            catalogTables = seaTunnelSource.getProducedCatalogTables();
        } catch (UnsupportedOperationException e) {
            // TODO remove it when all connector use `getProducedCatalogTables`
            SeaTunnelDataType<?> seaTunnelDataType = seaTunnelSource.getProducedType();
            catalogTables =
                    CatalogTableUtil.convertDataTypeToCatalogTables(seaTunnelDataType, "default");
        }
        MultiTableManager multiTableManager =
                new MultiTableManager(catalogTables.toArray(new CatalogTable[0]));
        return new BatchSourceReader(
                seaTunnelSource, applicationId, parallelism, envOptions, multiTableManager);
    }

    @Override
    public MicroBatchReader createMicroBatchReader(
            Optional<StructType> rowTypeOptional,
            String checkpointLocation,
            DataSourceOptions options) {
        SeaTunnelSource<SeaTunnelRow, ?, ?> seaTunnelSource = getSeaTunnelSource(options);
        Integer parallelism = options.getInt(EnvCommonOptions.PARALLELISM.key(), 1);
        String applicationId = SparkSession.getActiveSession().get().sparkContext().applicationId();
        Integer checkpointInterval =
                options.getInt(
                        EnvCommonOptions.CHECKPOINT_INTERVAL.key(), CHECKPOINT_INTERVAL_DEFAULT);
        String checkpointPath =
                StringUtils.replacePattern(checkpointLocation, "sources/\\d+", "sources-state");
        Configuration configuration =
                SparkSession.getActiveSession().get().sparkContext().hadoopConfiguration();
        String hdfsRoot =
                options.get(Constants.HDFS_ROOT)
                        .orElse(FileSystem.getDefaultUri(configuration).toString());
        String hdfsUser = options.get(Constants.HDFS_USER).orElse("");
        Integer checkpointId = options.getInt(Constants.CHECKPOINT_ID, 1);
        Map<String, String> envOptions = options.asMap();
        List<CatalogTable> catalogTables;
        try {
            catalogTables = seaTunnelSource.getProducedCatalogTables();
        } catch (UnsupportedOperationException e) {
            // TODO remove it when all connector use `getProducedCatalogTables`
            SeaTunnelDataType<?> seaTunnelDataType = seaTunnelSource.getProducedType();
            catalogTables =
                    CatalogTableUtil.convertDataTypeToCatalogTables(seaTunnelDataType, "default");
        }
        MultiTableManager multiTableManager =
                new MultiTableManager(catalogTables.toArray(new CatalogTable[0]));
        return new MicroBatchSourceReader(
                seaTunnelSource,
                parallelism,
                applicationId,
                checkpointId,
                checkpointInterval,
                checkpointPath,
                hdfsRoot,
                hdfsUser,
                envOptions,
                multiTableManager);
    }

    private SeaTunnelSource<SeaTunnelRow, ?, ?> getSeaTunnelSource(DataSourceOptions options) {
        return SerializationUtils.stringToObject(
                options.get(Constants.SOURCE_SERIALIZATION)
                        .orElseThrow(
                                () ->
                                        new UnsupportedOperationException(
                                                "Serialization information for the SeaTunnelSource is required")));
    }
}
