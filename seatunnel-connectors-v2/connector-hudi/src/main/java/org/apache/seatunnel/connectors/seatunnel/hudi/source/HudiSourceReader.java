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

package org.apache.seatunnel.connectors.seatunnel.hudi.source;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.hudi.exception.HudiConnectorException;
import org.apache.seatunnel.connectors.seatunnel.hudi.util.HudiUtil;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hudi.hadoop.HoodieParquetInputFormat;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;

public class HudiSourceReader implements SourceReader<SeaTunnelRow, HudiSourceSplit> {

    private static final long THREAD_WAIT_TIME = 500L;

    private final String confPaths;

    private final Set<HudiSourceSplit> sourceSplits;

    private final SourceReader.Context context;

    private final SeaTunnelRowType seaTunnelRowType;

    public HudiSourceReader(String confPaths, SourceReader.Context context, SeaTunnelRowType seaTunnelRowType) {
        this.confPaths = confPaths;
        this.context = context;
        this.sourceSplits = new HashSet<>();
        this.seaTunnelRowType = seaTunnelRowType;
    }

    @Override
    public void open() {
    }

    @Override
    public void close() {

    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        if (sourceSplits.isEmpty()) {
            Thread.sleep(THREAD_WAIT_TIME);
            return;
        }
        Configuration configuration = HudiUtil.getConfiguration(this.confPaths);
        JobConf jobConf = HudiUtil.toJobConf(configuration);
        sourceSplits.forEach(source -> {
            try {
                HoodieParquetInputFormat inputFormat = new HoodieParquetInputFormat();
                RecordReader<NullWritable, ArrayWritable> reader = inputFormat.getRecordReader(source.getInputSplit(), jobConf, Reporter.NULL);
                ParquetHiveSerDe serde = new ParquetHiveSerDe();
                Properties properties = new Properties();
                List<String> types = new ArrayList<>();
                for (SeaTunnelDataType<?> type : seaTunnelRowType.getFieldTypes()) {
                    types.add(type.getSqlType().name());
                }
                String columns = StringUtils.join(seaTunnelRowType.getFieldNames(), ",");
                String columnTypes = StringUtils.join(types, ",").toLowerCase(Locale.ROOT);
                properties.setProperty("columns", columns);
                properties.setProperty("columns.types", columnTypes);
                serde.initialize(jobConf, properties);
                StructObjectInspector inspector = (StructObjectInspector) serde.getObjectInspector();
                List<? extends StructField> fields = inspector.getAllStructFieldRefs();
                NullWritable key = reader.createKey();
                ArrayWritable value = reader.createValue();
                while (reader.next(key, value)) {
                    Object[] datas = new Object[fields.size()];
                    for (int i = 0; i < fields.size(); i++) {
                        Object data = inspector.getStructFieldData(value, fields.get(i));
                        if (null != data) {
                            datas[i] = String.valueOf(data);
                        } else {
                            datas[i] = null;
                        }
                    }
                    output.collect(new SeaTunnelRow(datas));
                }
                reader.close();
            } catch (Exception e) {
                throw new HudiConnectorException(CommonErrorCode.READER_OPERATION_FAILED, e);
            }
        });
        context.signalNoMoreElement();
    }

    @Override
    public List<HudiSourceSplit> snapshotState(long checkpointId) {
        return new ArrayList<>(sourceSplits);
    }

    @Override
    public void addSplits(List<HudiSourceSplit> splits) {
        sourceSplits.addAll(splits);
    }

    @Override
    public void handleNoMoreSplits() {

    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {

    }
}
