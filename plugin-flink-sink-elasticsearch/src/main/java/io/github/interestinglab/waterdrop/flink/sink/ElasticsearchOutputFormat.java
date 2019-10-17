package io.github.interestinglab.waterdrop.flink.sink;

import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;


public class ElasticsearchOutputFormat<Row> extends RichOutputFormat<Row> {

    @Override
    public void configure(Configuration configuration) {

    }

    @Override
    public void open(int i, int i1) {

    }

    @Override
    public void writeRecord(Row row) {

    }

    @Override
    public void close() {

    }

}
