package org.apache.seatunnel.app.domain.response;

import io.swagger.annotations.ApiModel;

import java.util.List;

@ApiModel(value = "pageInfo", description = "page info")
@SuppressWarnings("MagicNumber")
public class PageInfo<T> {
    private List<T> data;
    private Integer totalCount = 0;
    private Integer totalPage = 0;
    private Integer pageNo = 1;
    private Integer pageSize = 20;

    public List<T> getData() {
        return data;
    }

    public void setData(List<T> data) {
        this.data = data;
    }

    public Integer getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(Integer totalCount) {
        this.totalCount = totalCount;

        if (pageSize == null || pageSize == 0) {
            pageSize = 20;
        }
        if (this.totalCount % this.pageSize == 0) {
            this.totalPage = this.totalCount / this.pageSize == 0 ? 1 : this.totalCount / this.pageSize;
        }
        this.totalPage = this.totalCount / this.pageSize + 1;
    }

    public Integer getTotalPage() {
        return totalPage;
    }

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
