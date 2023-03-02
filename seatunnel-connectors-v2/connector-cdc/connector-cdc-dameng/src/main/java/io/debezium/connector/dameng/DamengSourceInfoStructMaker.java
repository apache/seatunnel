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

package io.debezium.connector.dameng;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.AbstractSourceInfoStructMaker;

public class DamengSourceInfoStructMaker extends AbstractSourceInfoStructMaker<SourceInfo> {
    private final Schema schema;

    public DamengSourceInfoStructMaker(
            String connector, String version, CommonConnectorConfig connectorConfig) {
        super(connector, version, connectorConfig);
        schema =
                commonSchemaBuilder()
                        .name("io.debezium.connector.dameng.Source")
                        .field(SourceInfo.SCHEMA_NAME_KEY, Schema.STRING_SCHEMA)
                        .field(SourceInfo.TABLE_NAME_KEY, Schema.STRING_SCHEMA)
                        .field(SourceInfo.SCN_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                        .field(SourceInfo.COMMIT_SCN_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                        .field(SourceInfo.EVENT_SERIAL_NO_KEY, Schema.OPTIONAL_INT64_SCHEMA)
                        .build();
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public Struct struct(SourceInfo sourceInfo) {
        String lsn = sourceInfo.getScn() == null ? null : sourceInfo.getScn().toString();
        Struct ret =
                super.commonStruct(sourceInfo)
                        .put(SourceInfo.SCHEMA_NAME_KEY, sourceInfo.tableSchema())
                        .put(SourceInfo.TABLE_NAME_KEY, sourceInfo.table())
                        .put(SourceInfo.SCN_KEY, lsn);

        if (sourceInfo.getCommitScn() != null) {
            ret.put(SourceInfo.COMMIT_SCN_KEY, sourceInfo.getCommitScn().toString());
        }
        return ret;
    }
}
