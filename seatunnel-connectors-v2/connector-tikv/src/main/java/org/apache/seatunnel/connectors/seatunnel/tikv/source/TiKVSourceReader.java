package org.apache.seatunnel.connectors.seatunnel.tikv.source;

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.tikv.config.ClientSession;
import org.apache.seatunnel.connectors.seatunnel.tikv.config.TiKVParameters;
import org.tikv.raw.RawKVClient;

import java.io.IOException;
import java.util.Objects;

/**
 * todo 2pc
 *
 * @author XuJiaWei
 * @since 2022-09-15 18:30
 */
public class TiKVSourceReader extends AbstractSingleSplitReader<SeaTunnelRow> {

    private ClientSession clientSession;

    private final TiKVParameters tikvParameters;

    private final SingleSplitReaderContext context;

    private final DeserializationSchema<SeaTunnelRow> deserializationSchema;

    public TiKVSourceReader(TiKVParameters tikvParameters,
                            SingleSplitReaderContext context,
                            DeserializationSchema<SeaTunnelRow> deserializationSchema) {
        this.tikvParameters = tikvParameters;
        this.context = context;
        this.deserializationSchema = deserializationSchema;
    }

    @Override
    public void open() {
        this.clientSession = new ClientSession(tikvParameters);
    }

    @Override
    public void close() {
        if (Objects.nonNull(clientSession)) {
            try {
                clientSession.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) {
        // create session client
        RawKVClient client = clientSession.session.createRawClient();
        // by the key get the value
        tikvParameters.getTikvDataType()
                .get(client, tikvParameters)
                .forEach(value -> collectResult(output, value));
        // termination signal
        context.signalNoMoreElement();
    }

    private void collectResult(Collector<SeaTunnelRow> output, String value) {
        if (deserializationSchema == null) {
            output.collect(new SeaTunnelRow(new Object[]{value}));
        } else {
            try {
                deserializationSchema.deserialize(value.getBytes(), output);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
