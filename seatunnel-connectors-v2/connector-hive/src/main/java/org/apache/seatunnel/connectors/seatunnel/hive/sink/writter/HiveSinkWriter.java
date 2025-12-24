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

package org.apache.seatunnel.connectors.seatunnel.hive.sink.writter;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.sink.BaseFileSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.file.sink.state.FileSinkState;
import org.apache.seatunnel.connectors.seatunnel.file.sink.writer.WriteStrategy;

import java.util.Collections;
import java.util.List;

public class HiveSinkWriter extends BaseFileSinkWriter
        implements SupportMultiTableSinkWriter<WriteStrategy> {

    public HiveSinkWriter(
            WriteStrategy writeStrategy,
            HadoopConf hadoopConf,
            Context context,
            String jobId,
            List<FileSinkState> fileSinkStates) {
        // todo: do we need to set writeStrategy as share resource? then how to deal with the pre
        // fileSinkStates?
        super(writeStrategy, hadoopConf, context, jobId, fileSinkStates);
    }

    public HiveSinkWriter(
            WriteStrategy writeStrategy,
            HadoopConf hadoopConf,
            SinkWriter.Context context,
            String jobId) {
        this(writeStrategy, hadoopConf, context, jobId, Collections.emptyList());
    }
}
