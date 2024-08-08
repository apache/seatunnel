package org.apache.seatunnel.connectors.seatunnel.sls.serialization;

import com.aliyun.openservices.log.common.LogGroupData;
import org.apache.seatunnel.api.source.Collector;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

public interface FastLogDeserialization<T> extends Serializable {

    default void deserialize(List<LogGroupData> logGroupDatas, Collector<T> out) throws IOException {

    }

}
