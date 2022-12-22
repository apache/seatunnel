package org.apache.seatunnel.connectors.seatunnel.file.config;

import java.io.Serializable;

public enum CompressFormat implements Serializable {

    LZO("lzo");

    private final String compressCodec;

    CompressFormat(String compressCodec) {
        this.compressCodec = compressCodec;
    }

    public String getCompressCodec() {
        return compressCodec;
    }
}
