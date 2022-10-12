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
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FilePluginException;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public abstract class AbstractReadStrategy implements ReadStrategy {
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
        if (hadoopConf != null) {
            configuration.set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY, hadoopConf.getHdfsNameKey());
            configuration.set("fs.hdfs.impl", hadoopConf.getFsHdfsImpl());
            hadoopConf.setExtraOptionsForConfiguration(configuration);
        }
        return configuration;
    }

    Configuration getConfiguration() throws FilePluginException {
        if (null == hadoopConf) {
            log.info("Local file reader didn't need hadoopConf");
        }
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
        if (pluginConfig.hasPath(BaseSourceConfig.PARSE_PARTITION_FROM_PATH)) {
            isMergePartition = pluginConfig.getBoolean(BaseSourceConfig.PARSE_PARTITION_FROM_PATH);
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

    protected SeaTunnelRow mergePartitionFields(Map<String, String> partitionsMap, SeaTunnelRow seaTunnelRow) {
        // get all values of partition fields
        Object[] partitions = partitionsMap.values().toArray(new Object[0]);
        // get all values of origin SeaTunnelRow
        Object[] fields = seaTunnelRow.getFields();
        // create a new array to merge partition fields and origin fields
        Object[] objects = new Object[fields.length + partitions.length];
        // copy origin values to new array
        System.arraycopy(fields, 0, objects, 0, fields.length);
        // copy partitions values to new array
        System.arraycopy(partitions, 0, objects, fields.length, partitions.length);
        // return merge row
        return new SeaTunnelRow(objects);
    }

    protected SeaTunnelRowType mergePartitionTypes(String path, SeaTunnelRowType seaTunnelRowType) {
        Map<String, String> partitionsMap = parsePartitionsByPath(path);
        if (partitionsMap.isEmpty()) {
            return seaTunnelRowType;
        }
        // get all names of partitions fields
        String[] partitionNames = partitionsMap.keySet().toArray(new String[0]);
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
