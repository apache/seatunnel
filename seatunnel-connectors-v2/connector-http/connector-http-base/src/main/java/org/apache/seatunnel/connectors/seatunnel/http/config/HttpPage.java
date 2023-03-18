package org.apache.seatunnel.connectors.seatunnel.http.config;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;

@Data
@Builder
public class HttpPage implements Serializable {

    private String pageNum;
    private String pageField;
    private String paheSize;
}
