package org.apache.seatunnel.connectors.seatunnel.typesense.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;
import java.util.Map;

@Data
@AllArgsConstructor
public class SourceCollectionInfo implements Serializable {
    private String collection;
    // TODO 暂时未使用 , 查询条件过滤
    private Map<String, Object> query;
    private long found;
    private int offset = 0;
}
