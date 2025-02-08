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

package org.apache.seatunnel.connectors.seatunnel.file.config;

import org.apache.seatunnel.connectors.seatunnel.file.sink.config.FileSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.file.sink.writer.BinaryWriteStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.sink.writer.CsvWriteStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.sink.writer.ExcelWriteStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.sink.writer.JsonWriteStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.sink.writer.OrcWriteStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.sink.writer.ParquetWriteStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.sink.writer.TextWriteStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.sink.writer.WriteStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.sink.writer.XmlWriteStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.source.reader.BinaryReadStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.source.reader.CsvReadStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.source.reader.ExcelReadStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.source.reader.JsonReadStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.source.reader.OrcReadStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.source.reader.ParquetReadStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.source.reader.ReadStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.source.reader.TextReadStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.source.reader.XmlReadStrategy;

import java.io.Serializable;
import java.util.Arrays;

public enum FileFormat implements Serializable {
    CSV("csv") {
        @Override
        public WriteStrategy getWriteStrategy(FileSinkConfig fileSinkConfig) {
            fileSinkConfig.setFieldDelimiter(",");
            return new CsvWriteStrategy(fileSinkConfig);
        }

        @Override
        public ReadStrategy getReadStrategy() {
            return new CsvReadStrategy();
        }
    },
    TEXT("txt") {
        @Override
        public WriteStrategy getWriteStrategy(FileSinkConfig fileSinkConfig) {
            return new TextWriteStrategy(fileSinkConfig);
        }

        @Override
        public ReadStrategy getReadStrategy() {
            return new TextReadStrategy();
        }
    },
    PARQUET("parquet") {
        @Override
        public WriteStrategy getWriteStrategy(FileSinkConfig fileSinkConfig) {
            return new ParquetWriteStrategy(fileSinkConfig);
        }

        @Override
        public ReadStrategy getReadStrategy() {
            return new ParquetReadStrategy();
        }
    },
    ORC("orc") {
        @Override
        public WriteStrategy getWriteStrategy(FileSinkConfig fileSinkConfig) {
            return new OrcWriteStrategy(fileSinkConfig);
        }

        @Override
        public ReadStrategy getReadStrategy() {
            return new OrcReadStrategy();
        }
    },
    JSON("json") {
        @Override
        public WriteStrategy getWriteStrategy(FileSinkConfig fileSinkConfig) {
            return new JsonWriteStrategy(fileSinkConfig);
        }

        @Override
        public ReadStrategy getReadStrategy() {
            return new JsonReadStrategy();
        }
    },
    EXCEL("xlsx", "xls") {
        @Override
        public WriteStrategy getWriteStrategy(FileSinkConfig fileSinkConfig) {
            return new ExcelWriteStrategy(fileSinkConfig);
        }

        @Override
        public ReadStrategy getReadStrategy() {
            return new ExcelReadStrategy();
        }
    },
    XML("xml") {
        @Override
        public WriteStrategy getWriteStrategy(FileSinkConfig fileSinkConfig) {
            return new XmlWriteStrategy(fileSinkConfig);
        }

        @Override
        public ReadStrategy getReadStrategy() {
            return new XmlReadStrategy();
        }
    },
    BINARY("") {
        @Override
        public WriteStrategy getWriteStrategy(FileSinkConfig fileSinkConfig) {
            return new BinaryWriteStrategy(fileSinkConfig);
        }

        @Override
        public ReadStrategy getReadStrategy() {
            return new BinaryReadStrategy();
        }
    };

    private final String[] suffix;

    FileFormat(String... suffix) {
        this.suffix = suffix;
    }

    public String getSuffix() {
        if (suffix.length > 0) {
            return "." + suffix[0];
        }
        return "";
    }

    public String[] getAllSuffix() {
        return Arrays.stream(suffix).map(suffix -> "." + suffix).toArray(String[]::new);
    }

    public ReadStrategy getReadStrategy() {
        return null;
    }

    public WriteStrategy getWriteStrategy(FileSinkConfig fileSinkConfig) {
        return null;
    }
}
