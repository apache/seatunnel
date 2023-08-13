package org.apache.seatunnel.connectors.seatunnel.access.sink;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.access.client.AccessClient;
import org.apache.seatunnel.connectors.seatunnel.access.config.AccessParameters;
import org.apache.seatunnel.connectors.seatunnel.access.exception.AccessConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.access.exception.AccessConnectorException;
import org.apache.seatunnel.connectors.seatunnel.access.util.TypeConvertUtil;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;

@Slf4j
public class AccessSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {
    private final AccessParameters accessParameters;
    private Connection connection;
    private final PreparedStatement statement;
    private final SeaTunnelDataType<?>[] seaTunnelDataTypes;

    public AccessSinkWriter(
            AccessParameters accessParameters, SeaTunnelDataType<?>[] seaTunnelDataTypes) {
        this.accessParameters = accessParameters;
        this.seaTunnelDataTypes = seaTunnelDataTypes;
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
        try {
            this.statement = connection.prepareStatement(initPrepareCQL());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        try {
            for (int i = 0; i < accessParameters.getFields().size(); i++) {
                String type = this.seaTunnelDataTypes[i].toString();
                TypeConvertUtil.reconvertAndInject(statement, i, type, element.getField(i));
            }
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            if (this.connection != null) {
                this.connection.close();
            }
        } catch (Exception e) {
            throw new AccessConnectorException(
                    AccessConnectorErrorCode.CLOSE_CQL_CONNECT_FAILED, e);
        }
    }

    private String initPrepareCQL() {
        String[] placeholder = new String[accessParameters.getFields().size()];
        Arrays.fill(placeholder, "?");
        return String.format(
                "INSERT INTO %s (%s) VALUES (%s)",
                accessParameters.getTable(),
                String.join(",", accessParameters.getFields()),
                String.join(",", placeholder));
    }
}
