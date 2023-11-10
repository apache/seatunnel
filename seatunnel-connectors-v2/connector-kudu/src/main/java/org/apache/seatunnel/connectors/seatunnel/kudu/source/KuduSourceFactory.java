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

package org.apache.seatunnel.connectors.seatunnel.kudu.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.schema.TableSchemaOptions;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.common.utils.ExceptionUtils;
import org.apache.seatunnel.connectors.seatunnel.kudu.config.KuduSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.kudu.config.KuduSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.kudu.exception.KuduConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.kudu.exception.KuduConnectorException;
import org.apache.seatunnel.connectors.seatunnel.kudu.kuduclient.KuduInputFormat;
import org.apache.seatunnel.connectors.seatunnel.kudu.kuduclient.KuduTypeMapper;
import org.apache.seatunnel.connectors.seatunnel.kudu.util.KuduUtil;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.client.KuduClient;

import com.google.auto.service.AutoService;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static org.apache.seatunnel.connectors.seatunnel.kudu.config.KuduSourceConfig.MASTER;
import static org.apache.seatunnel.connectors.seatunnel.kudu.config.KuduSourceConfig.TABLE_NAME;

@AutoService(Factory.class)
public class KuduSourceFactory implements TableSourceFactory {

    @Override
    public String factoryIdentifier() {
        return "Kudu";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(MASTER, TABLE_NAME)
                .optional(TableSchemaOptions.SCHEMA)
                .optional(KuduSourceConfig.WORKER_COUNT)
                .optional(KuduSourceConfig.OPERATION_TIMEOUT)
                .optional(KuduSourceConfig.ADMIN_OPERATION_TIMEOUT)
                .optional(KuduSourceConfig.QUERY_TIMEOUT)
                .optional(KuduSourceConfig.SCAN_BATCH_SIZE_BYTES)
                .optional(KuduSourceConfig.FILTER)
                .optional(KuduSinkConfig.ENABLE_KERBEROS)
                .optional(KuduSinkConfig.KERBEROS_KRB5_CONF)
                .conditional(
                        KuduSinkConfig.ENABLE_KERBEROS,
                        true,
                        KuduSinkConfig.KERBEROS_PRINCIPAL,
                        KuduSinkConfig.KERBEROS_KEYTAB)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return KuduSource.class;
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        ReadonlyConfig config = context.getOptions();
        KuduSourceConfig kuduSourceConfig = new KuduSourceConfig(config);
        SeaTunnelRowType rowTypeInfo;
        if (config.getOptional(TableSchemaOptions.SCHEMA).isPresent()) {
            rowTypeInfo = CatalogTableUtil.buildWithConfig(config).getSeaTunnelRowType();
        } else {
            try (KuduClient kuduClient = KuduUtil.getKuduClient(kuduSourceConfig)) {
                rowTypeInfo =
                        getSeaTunnelRowType(
                                kuduClient
                                        .openTable(kuduSourceConfig.getTable())
                                        .getSchema()
                                        .getColumns());
            } catch (Exception e) {
                throw new KuduConnectorException(KuduConnectorErrorCode.INIT_KUDU_CLIENT_FAILED, e);
            }
        }
        KuduInputFormat kuduInputFormat = new KuduInputFormat(kuduSourceConfig, rowTypeInfo);

        return () ->
                (SeaTunnelSource<T, SplitT, StateT>)
                        new KuduSource(kuduSourceConfig, kuduInputFormat);
    }

    public static SeaTunnelRowType getSeaTunnelRowType(List<ColumnSchema> columnSchemaList) {
        ArrayList<SeaTunnelDataType<?>> seaTunnelDataTypes = new ArrayList<>();
        ArrayList<String> fieldNames = new ArrayList<>();
        try {

            for (int i = 0; i < columnSchemaList.size(); i++) {
                fieldNames.add(columnSchemaList.get(i).getName());
                seaTunnelDataTypes.add(KuduTypeMapper.mapping(columnSchemaList, i));
            }

        } catch (Exception e) {
            throw new KuduConnectorException(
                    CommonErrorCodeDeprecated.TABLE_SCHEMA_GET_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            "Kudu", PluginType.SOURCE, ExceptionUtils.getMessage(e)));
        }
        return new SeaTunnelRowType(
                fieldNames.toArray(new String[0]),
                seaTunnelDataTypes.toArray(new SeaTunnelDataType<?>[0]));
    }
}
