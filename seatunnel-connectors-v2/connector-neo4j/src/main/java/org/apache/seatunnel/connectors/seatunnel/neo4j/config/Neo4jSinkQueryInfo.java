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

package org.apache.seatunnel.connectors.seatunnel.neo4j.config;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.connectors.seatunnel.neo4j.constants.SinkWriteMode;

import lombok.Getter;
import lombok.Setter;

import java.util.Map;

import static org.apache.seatunnel.connectors.seatunnel.neo4j.config.Neo4jSinkOptions.MAX_BATCH_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.neo4j.config.Neo4jSinkOptions.QUERY_PARAM_POSITION;
import static org.apache.seatunnel.connectors.seatunnel.neo4j.config.Neo4jSinkOptions.WRITE_MODE;

@Getter
@Setter
public class Neo4jSinkQueryInfo extends Neo4jQueryInfo {

    private Map<String, Object> queryParamPosition;
    private Integer maxBatchSize;

    private SinkWriteMode writeMode;

    public boolean batchMode() {
        return SinkWriteMode.BATCH.equals(writeMode);
    }

    public Neo4jSinkQueryInfo(Config config) {
        super(config);

        this.writeMode = prepareWriteMode(config);

        if (SinkWriteMode.BATCH.equals(writeMode)) {
            prepareBatchWriteConfig(config);
        } else {
            prepareOneByOneConfig(config);
        }
    }

    private void prepareOneByOneConfig(Config config) {
        // set queryParamPosition
        this.queryParamPosition = config.getObject(QUERY_PARAM_POSITION.key()).unwrapped();
    }

    private void prepareBatchWriteConfig(Config config) {
        // batch size
        if (config.hasPath(MAX_BATCH_SIZE.key())) {
            this.maxBatchSize = config.getInt(MAX_BATCH_SIZE.key());
        } else {
            this.maxBatchSize = MAX_BATCH_SIZE.defaultValue();
        }
    }

    private SinkWriteMode prepareWriteMode(Config config) {
        if (config.hasPath(WRITE_MODE.key())) {
            return config.getEnum(SinkWriteMode.class, WRITE_MODE.key());
        }
        return WRITE_MODE.defaultValue();
    }
}
