package org.apache.seatunnel.server.common;

import lombok.Data;

import java.util.Collections;
import java.util.List;

@Data
public class PageData<T> {
    private int totalCount;
    private List<T> data;

    public PageData(int totalCount, List<T> data) {
        this.totalCount = totalCount;
        this.data = data;
    }

    public static <T> PageData<T> empty() {
        return new PageData<>(0, Collections.emptyList());
    }

    public List<T> getData() {
        if (data == null || data.size() == 0) {
            return Collections.emptyList();
        }
        return data;
    }
}
