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

package org.apache.seatunnel.connectors.seatunnel.doris.source;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelContext;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitSource;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.doris.util.JDBCUtils;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.auto.service.AutoService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;

@AutoService(SeaTunnelSource.class)
public class DorisSource extends AbstractSingleSplitSource<SeaTunnelRow> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DorisSource.class);
    private SeaTunnelContext seaTunnelContext;
    private SeaTunnelRowType rowTypeInfo;
    private DorisInputFormat dorisInputFormat;

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SeaTunnelRowType getProducedType() {
        return this.rowTypeInfo;
    }

    @Override
    public AbstractSingleSplitReader<SeaTunnelRow> createReader(SingleSplitReaderContext readerContext) throws Exception {
        return new DorisSourceReader(readerContext, dorisInputFormat);
    }

    @Override
    public String getPluginName() {
        return "DorisSource";
    }

    @Override
    public void prepare(Config config) {
        String dorisbeaddress = "";
        String password = "";
        String username = "";
        String selectsql = "";
        String database = "";
        if (config.hasPath(DorisSourceParameter.DORISBEADDRESS) && config.hasPath(DorisSourceParameter.PASSWORD)
            && config.hasPath(DorisSourceParameter.USERNAME) && config.hasPath(DorisSourceParameter.SELECTSQL) && config.hasPath(DorisSourceParameter.DATABASE)) {
            dorisbeaddress = config.getString(DorisSourceParameter.DORISBEADDRESS);
            password = config.getString(DorisSourceParameter.PASSWORD);
            username = config.getString(DorisSourceParameter.USERNAME);
            selectsql = config.getString(DorisSourceParameter.SELECTSQL);
            database = config.getString(DorisSourceParameter.DATABASE);
            try {

                SeaTunnelRowType seaTunnelRowType = getSeaTunnelRowType(JDBCUtils.getMetaData(JDBCUtils.getConnection(dorisbeaddress, username, password, database), selectsql));
                rowTypeInfo = seaTunnelRowType;
            } catch (SQLException e) {
                throw new RuntimeException("Parameters in the preparation phase fail to be generated", e);
            }
            dorisInputFormat = new DorisInputFormat(dorisbeaddress, password, username, selectsql, database, rowTypeInfo);
        } else {
            throw new RuntimeException("Missing Source configuration parameters");
        }

    }

    @Override
    public void setSeaTunnelContext(SeaTunnelContext seaTunnelContext) {
        this.seaTunnelContext = seaTunnelContext;
    }

    public SeaTunnelRowType getSeaTunnelRowType(ResultSetMetaData columnSchemaList) {
        ArrayList<SeaTunnelDataType<?>> seaTunnelDataTypes = new ArrayList<>();
        ArrayList<String> fieldNames = new ArrayList<>();
        try {

            for (int i = 1; i <= columnSchemaList.getColumnCount(); i++) {
                fieldNames.add(columnSchemaList.getColumnName(i));
                seaTunnelDataTypes.add(DorisTypeMapper.mapping(columnSchemaList, i));
            }

        } catch (Exception e) {
            LOGGER.warn("get row type info exception", e);
            throw new PrepareFailException("Doris", PluginType.SOURCE, e.toString());
        }
        return new SeaTunnelRowType(fieldNames.toArray(new String[fieldNames.size()]), seaTunnelDataTypes.toArray(new SeaTunnelDataType<?>[seaTunnelDataTypes.size()]));
    }
}
