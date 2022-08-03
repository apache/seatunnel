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

package org.apache.seatunnel.connectors.seatunnel.hive.source.file.reader.format;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.hive.exception.HivePluginException;
import org.apache.seatunnel.connectors.seatunnel.hive.source.HadoopConf;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class ParquetReadStrategy extends AbstractReadStrategy {

    private SeaTunnelRowType seaTunnelRowType;

    private static final byte[] PARQUET_MAGIC = new byte[]{(byte) 'P', (byte) 'A', (byte) 'R', (byte) '1'};

    @Override
    public void read(String path, Collector<SeaTunnelRow> output) throws Exception {
        if (Boolean.FALSE.equals(checkFileType(path))) {
            throw new Exception("please check file type");
        }
        Path filePath = new Path(path);
        HadoopInputFile hadoopInputFile = HadoopInputFile.fromPath(filePath, getConfiguration());
        int fieldsCount = seaTunnelRowType.getTotalFields();
        GenericRecord record;
        try (ParquetReader<GenericData.Record> reader = AvroParquetReader.<GenericData.Record>builder(hadoopInputFile).build()) {
            while ((record = reader.read()) != null) {
                Object[] fields = new Object[fieldsCount];
                for (int i = 0; i < fieldsCount; i++) {
                    Object data = record.get(i);
                    try {
                        if (data instanceof GenericData.Fixed) {
                            // judge the data in upstream is or not decimal type
                            data = fixed2String((GenericData.Fixed) data);
                        } else if (data instanceof ArrayList) {
                            // judge the data in upstream is or not array type
                            data = array2String((ArrayList<GenericData.Record>) data);
                        }
                    } catch (Exception e) {
                        data = record.get(i);
                    } finally {
                        fields[i] = data.toString();
                    }
                }
                output.collect(new SeaTunnelRow(fields));
            }
        }
    }

    @Override
    public SeaTunnelRowType getSeaTunnelRowTypeInfo(HadoopConf hadoopConf, String path) throws HivePluginException {
        if (seaTunnelRowType != null) {
            return seaTunnelRowType;
        }
        Configuration configuration = getConfiguration(hadoopConf);
        Path filePath = new Path(path);
        ParquetMetadata metadata;
        try {
            metadata = ParquetFileReader.readFooter(configuration, filePath);
        } catch (IOException e) {
            throw new HivePluginException("Create parquet reader failed", e);
        }
        FileMetaData fileMetaData = metadata.getFileMetaData();
        MessageType schema = fileMetaData.getSchema();
        int fieldCount = schema.getFieldCount();
        String[] fields = new String[fieldCount];
        SeaTunnelDataType[] types = new SeaTunnelDataType[fieldCount];
        for (int i = 0; i < fieldCount; i++) {
            fields[i] = schema.getFieldName(i);
            // Temporarily each field is treated as a string type
            // I think we can use the schema information to build seatunnel column type
            types[i] = BasicType.STRING_TYPE;
        }
        seaTunnelRowType = new SeaTunnelRowType(fields, types);
        return seaTunnelRowType;
    }

    private String fixed2String(GenericData.Fixed fixed) {
        Schema schema = fixed.getSchema();
        byte[] bytes = fixed.bytes();
        int precision = Integer.parseInt(schema.getObjectProps().get("precision").toString());
        int scale = Integer.parseInt(schema.getObjectProps().get("scale").toString());
        BigDecimal bigDecimal = bytes2Decimal(bytes, precision, scale);
        return bigDecimal.toString();
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    private BigDecimal bytes2Decimal(byte[] bytesArray, int precision, int scale) {
        Binary value = Binary.fromConstantByteArray(bytesArray);
        if (precision <= 18) {
            ByteBuffer buffer = value.toByteBuffer();
            byte[] bytes = buffer.array();
            int start = buffer.arrayOffset() + buffer.position();
            int end = buffer.arrayOffset() + buffer.limit();
            long unscaled = 0L;
            int i = start;
            while (i < end) {
                unscaled = unscaled << 8 | bytes[i] & 0xff;
                i++;
            }
            int bits = 8 * (end - start);
            long unscaledNew = (unscaled << (64 - bits)) >> (64 - bits);
            if (unscaledNew <= -Math.pow(10, 18) || unscaledNew >= Math.pow(10, 18)) {
                return new BigDecimal(unscaledNew);
            } else {
                return BigDecimal.valueOf(unscaledNew / Math.pow(10, scale));
            }
        } else {
            return new BigDecimal(new BigInteger(value.getBytes()), scale);
        }
    }

    @Override
    boolean checkFileType(String path) {
        boolean checkResult;
        byte[] magic = new byte[PARQUET_MAGIC.length];
        try {
            Configuration configuration = getConfiguration();
            FileSystem fileSystem = FileSystem.get(configuration);
            Path filePath = new Path(path);
            FSDataInputStream in = fileSystem.open(filePath);
            // try to get header information in a parquet file
            in.seek(0);
            in.readFully(magic);
            checkResult = Arrays.equals(magic, PARQUET_MAGIC);
            in.close();
            return checkResult;
        } catch (HivePluginException | IOException e) {
            String errorMsg = String.format("Check parquet file [%s] error", path);
            throw new RuntimeException(errorMsg, e);
        }
    }

    private String array2String(ArrayList<GenericData.Record> data) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        List<String> values = data.stream().map(record -> record.get(0).toString()).collect(Collectors.toList());
        return objectMapper.writeValueAsString(values);
    }
}
