package org.apache.seatunnel.connectors.seatunnel.http.config;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

@Setter
@Getter
@ToString
public class PageInfo implements Serializable {

    private Long totalPageSize;
    private String jsonVerifyExpression;
    private String jsonVerifyValue;
    private Long maxPageSize;
    private String pageField;
    private Long pageIndex;
    private String totalPageFieldPath;
}
