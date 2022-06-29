package org.apache.seatunnel.admin.dto;

import io.swagger.annotations.ApiModelProperty;

import java.io.Serializable;

public class BasePage implements Serializable {

    @ApiModelProperty(name = "pageNo", value = "PAGENO", dataType = "Int", example = "1")
    private Integer pageNo = 1;
    @ApiModelProperty(name = "pageSize", value = "PAGESIZE", dataType = "Int", example = "10")
    private Integer pageSize = 10;

    public Integer getPageNo() {
        return pageNo;
    }

    public void setPageNo(Integer pageNo) {
        this.pageNo = pageNo;
    }

    public Integer getPageSize() {
        return pageSize;
    }

    public void setPageSize(Integer pageSize) {
        this.pageSize = pageSize;
    }
}
