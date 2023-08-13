package org.apache.seatunnel.connectors.seatunnel.access.source;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.access.client.AccessClient;
import org.apache.seatunnel.connectors.seatunnel.access.config.AccessParameters;
import org.apache.seatunnel.connectors.seatunnel.access.util.TypeConvertUtil;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;


@Slf4j
public class AccessSourceReader extends AbstractSingleSplitReader<SeaTunnelRow> {

    private Connection connection;

    private final SingleSplitReaderContext readerContext;

    private final AccessParameters accessParameters;

    AccessSourceReader(AccessParameters accessParameters, SingleSplitReaderContext readerContext) {
        this.accessParameters = accessParameters;
        this.readerContext = readerContext;
    }

    @Override
    public void open() throws Exception {
        AccessClient accessClient =
                new AccessClient(
                        accessParameters.getDriver(),
                        accessParameters.getUrl(),
                        accessParameters.getUsername(),
                        accessParameters.getPassword(),
                        accessParameters.getQuery());
        connection =
                accessClient.getAccessConnection(
                        accessParameters.getUrl(),
                        accessParameters.getUsername(),
                        accessParameters.getPassword());
    }

    @Override
    public void close() throws IOException {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        Statement statement = connection.createStatement();
        ResultSet result = statement.executeQuery(accessParameters.getQuery());
        ResultSetMetaData metaData = result.getMetaData();

        while (result.next()) {
            Object[] datas = new Object[metaData.getColumnCount()];
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                String columnName = metaData.getColumnName(i);
                String columnType = metaData.getColumnTypeName(i);
                datas[i - 1] = TypeConvertUtil.convertToObject(result, columnName, columnType);
            }
            output.collect(new SeaTunnelRow(datas));
        }
        this.readerContext.signalNoMoreElement();
    }
}
