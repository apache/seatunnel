package org.apache.seatunnel.connectors.seatunnel.tikv.config;

import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;

/**
 * @author XuJiaWei
 * @since 2022-09-16 11:02
 */
public class ClientSession implements AutoCloseable {

    public final TiSession session;

    public ClientSession(TiKVParameters config) {
        // reference tiBigdata project
        TiConfiguration tiConfiguration = TiConfiguration.createDefault(config.getPdAddresses());
        this.session = TiSession.create(tiConfiguration);
    }

    @Override
    public void close() throws Exception {
        session.close();
    }

}
