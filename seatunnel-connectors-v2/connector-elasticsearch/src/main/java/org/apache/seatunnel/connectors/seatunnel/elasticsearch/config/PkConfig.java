package org.apache.seatunnel.connectors.seatunnel.elasticsearch.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

/** es source pk config */
@Data
@AllArgsConstructor
@Builder
public class PkConfig {
    private String name; // pk field name
    private String type; // like: long/keyword/integer
    private Integer length; // column length, when type is keyword need set length
}
