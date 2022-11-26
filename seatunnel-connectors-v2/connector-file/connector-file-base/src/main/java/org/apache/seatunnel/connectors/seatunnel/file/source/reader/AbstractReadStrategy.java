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

package org.apache.seatunnel.connectors.seatunnel.file.source.reader;

import static org.apache.parquet.avro.AvroReadSupport.READ_INT96_AS_FIXED;
import static org.apache.parquet.avro.AvroSchemaConverter.ADD_LIST_ELEMENT_RECORDS;
import static org.apache.parquet.avro.AvroWriteSupport.WRITE_FIXED_AS_INT96;
import static org.apache.parquet.avro.AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public abstract class AbstractReadStrategy implements ReadStrategy {
    protected static final String[] TYPE_ARRAY_STRING = new String[0];
    protected static final Boolean[] TYPE_ARRAY_BOOLEAN = new Boolean[0];
    protected static final Byte[] TYPE_ARRAY_BYTE = new Byte[0];
    protected static final Short[] TYPE_ARRAY_SHORT = new Short[0];
    protected static final Integer[] TYPE_ARRAY_INTEGER = new Integer[0];
    protected static final Long[] TYPE_ARRAY_LONG = new Long[0];
    protected static final Float[] TYPE_ARRAY_FLOAT = new Float[0];
    protected static final Double[] TYPE_ARRAY_DOUBLE = new Double[0];
    protected static final BigDecimal[] TYPE_ARRAY_BIG_DECIMAL = new BigDecimal[0];
    protected static final LocalDate[] TYPE_ARRAY_LOCAL_DATE = new LocalDate[0];
    protected static final LocalDateTime[] TYPE_ARRAY_LOCAL_DATETIME = new LocalDateTime[0];

    protected HadoopConf hadoopConf;
    protected SeaTunnelRowType seaTunnelRowType;
    protected SeaTunnelRowType seaTunnelRowTypeWithPartition;
    protected Config pluginConfig;
    protected List<String> fileNames = new ArrayList<>();
    protected boolean isMergePartition = true;

    @Override
    public void init(HadoopConf conf) {
        this.hadoopConf = conf;
    }

    @Override
    public void setSeaTunnelRowTypeInfo(SeaTunnelRowType seaTunnelRowType) {
        this.seaTunnelRowType = seaTunnelRowType;
        this.seaTunnelRowTypeWithPartition = mergePartitionTypes(fileNames.get(0), seaTunnelRowType);
    }

    @Override
    public Configuration getConfiguration(HadoopConf hadoopConf) {
        Configuration configuration = new Configuration();
        configuration.setBoolean(READ_INT96_AS_FIXED, true);
        configuration.setBoolean(WRITE_FIXED_AS_INT96, true);
        configuration.setBoolean(ADD_LIST_ELEMENT_RECORDS, false);
        configuration.setBoolean(WRITE_OLD_LIST_STRUCTURE, false);
        configuration.set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY, hadoopConf.getHdfsNameKey());
        configuration.set(String.format("fs.%s.impl", hadoopConf.getSchema()), hadoopConf.getFsHdfsImpl());
        hadoopConf.setExtraOptionsForConfiguration(configuration);
        return configuration;
    }

    Configuration getConfiguration() {
        return getConfiguration(hadoopConf);
    }

    boolean checkFileType(String path) {
        return true;
    }

    @Override
    public List<String> getFileNamesByPath(HadoopConf hadoopConf, String path) throws IOException {
        Configuration configuration = getConfiguration(hadoopConf);
        FileSystem hdfs = FileSystem.get(configuration);
        ArrayList<String> fileNames = new ArrayList<>();
        Path listFiles = new Path(path);
        FileStatus[] stats = hdfs.listStatus(listFiles);
        for (FileStatus fileStatus : stats) {
            if (fileStatus.isDirectory()) {
                fileNames.addAll(getFileNamesByPath(hadoopConf, fileStatus.getPath().toString()));
                continue;
            }
            if (fileStatus.isFile()) {
                // filter '_SUCCESS' file
                if (!fileStatus.getPath().getName().equals("_SUCCESS")) {
                    fileNames.add(fileStatus.getPath().toString());
                    this.fileNames.add(fileStatus.getPath().toString());
                }
            }
        }
        return fileNames;
    }

    @Override
    public void setPluginConfig(Config pluginConfig) {
        this.pluginConfig = pluginConfig;
        if (pluginConfig.hasPath(BaseSourceConfig.PARSE_PARTITION_FROM_PATH.key())) {
            isMergePartition = pluginConfig.getBoolean(BaseSourceConfig.PARSE_PARTITION_FROM_PATH.key());
        }
    }

    @Override
    public SeaTunnelRowType getActualSeaTunnelRowTypeInfo() {
        return isMergePartition ? seaTunnelRowTypeWithPartition : seaTunnelRowType;
    }

    protected Map<String, String> parsePartitionsByPath(String path) {
        LinkedHashMap<String, String> partitions = new LinkedHashMap<>();
        Arrays.stream(path.split("/", -1))
                .filter(split -> split.contains("="))
                .map(split -> split.split("=", -1))
                .forEach(kv -> partitions.put(kv[0], kv[1]));
        return partitions;
    }

    protected SeaTunnelRowType mergePartitionTypes(String path, SeaTunnelRowType seaTunnelRowType) {
        Map<String, String> partitionsMap = parsePartitionsByPath(path);
        if (partitionsMap.isEmpty()) {
            return seaTunnelRowType;
        }
        // get all names of partitions fields
        String[] partitionNames = partitionsMap.keySet().toArray(TYPE_ARRAY_STRING);
        // initialize data type for partition fields
        SeaTunnelDataType<?>[] partitionTypes = new SeaTunnelDataType<?>[partitionNames.length];
        Arrays.fill(partitionTypes, BasicType.STRING_TYPE);
        // get origin field names
        String[] fieldNames = seaTunnelRowType.getFieldNames();
        // get origin data types
        SeaTunnelDataType<?>[] fieldTypes = seaTunnelRowType.getFieldTypes();
        // create new array to merge partition fields and origin fields
        String[] newFieldNames = new String[fieldNames.length + partitionNames.length];
        // create new array to merge partition fields' data type and origin fields' data type
        SeaTunnelDataType<?>[] newFieldTypes = new SeaTunnelDataType<?>[fieldTypes.length + partitionTypes.length];
        // copy origin field names to new array
        System.arraycopy(fieldNames, 0, newFieldNames, 0, fieldNames.length);
        // copy partitions field name to new array
        System.arraycopy(partitionNames, 0, newFieldNames, fieldNames.length, partitionNames.length);
        // copy origin field types to new array
        System.arraycopy(fieldTypes, 0, newFieldTypes, 0, fieldTypes.length);
        // copy partition field types to new array
        System.arraycopy(partitionTypes, 0, newFieldTypes, fieldTypes.length, partitionTypes.length);
        // return merge row type
        return new SeaTunnelRowType(newFieldNames, newFieldTypes);
    }
}
