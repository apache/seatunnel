package org.apache.seatunnel.connectors.seatunnel.tikv.sink;

import org.apache.seatunnel.api.serialization.SerializationSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.tikv.config.ClientSession;
import org.apache.seatunnel.connectors.seatunnel.tikv.config.TiKVDataType;
import org.apache.seatunnel.connectors.seatunnel.tikv.config.TiKVParameters;
import org.apache.seatunnel.format.json.JsonSerializationSchema;
import org.tikv.raw.RawKVClient;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * todo 2pc
 *
 * @author XuJiaWei
 * @since 2022-09-15 18:12
 */
public class TiKVSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {

    private final ClientSession clientSession;
    private final TiKVParameters tikvParameters;
    private final SeaTunnelRowType seaTunnelRowType;
    private final SerializationSchema serializationSchema;

    public TiKVSinkWriter(SeaTunnelRowType seaTunnelRowType, TiKVParameters tikvParameters) {
        this.tikvParameters = tikvParameters;
        this.clientSession = new ClientSession(tikvParameters);
        this.seaTunnelRowType = seaTunnelRowType;
        this.serializationSchema = new JsonSerializationSchema(seaTunnelRowType);
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        RawKVClient client = clientSession.session.createRawClient();

        String data = new String(serializationSchema.serialize(element));
        TiKVDataType tikvDataType = tikvParameters.getTikvDataType();
        String keyField = tikvParameters.getKeyField();
        List<String> fields = Arrays.asList(seaTunnelRowType.getFieldNames());
        String key;
        if (fields.contains(keyField)) {
            key = element.getField(fields.indexOf(keyField)).toString();
        } else {
            key = keyField;
        }

        tikvDataType.set(client, key, data);
    }

    @Override
    public void close() throws IOException {
        try {
            clientSession.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
