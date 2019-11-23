package io.github.interestinglab.waterdrop.flink.sink;

import com.typesafe.config.waterdrop.Config;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;


public class ElasticsearchOutputFormat<T> extends RichOutputFormat<T> {

    private Config config;

    private final static String prefix = "";

    private final ElasticsearchSinkFunction<T> elasticsearchSinkFunction;

    private transient RequestIndexer requestIndexer;

    private transient BulkProcessor bulkProcessor;


    public ElasticsearchOutputFormat(Config userConfig, ElasticsearchSinkFunction<T> elasticsearchSinkFunction) {
        this.config = userConfig;
        this.elasticsearchSinkFunction = elasticsearchSinkFunction;
    }
    @Override
    public void configure(Configuration configuration) {

        Settings.Builder settings = Settings.builder();

        config.entrySet().forEach(entry -> {
            String key = entry.getKey();
            Object value = entry.getValue().unwrapped();
            if (key.startsWith(prefix)) {
                settings.put(key.substring(prefix.length()), value.toString());
            }
        });

        TransportClient transportClient = new PreBuiltTransportClient(settings.build());

        try {
            transportClient.addTransportAddresses(new TransportAddress(InetAddress.getByName("localhost"), 9300));
        } catch (Exception e) {
            e.printStackTrace();
        }
        BulkProcessor.Builder bulkProcessorBuilder = BulkProcessor.builder(transportClient, new BulkProcessor.Listener() {
            public void beforeBulk(long executionId, BulkRequest request) {

            }

            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {

            }

            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
            }
        });

        // This makes flush() blocking
        bulkProcessorBuilder.setConcurrentRequests(0);

        bulkProcessor = bulkProcessorBuilder.build();
        requestIndexer = new RequestIndexer() {
            @Override
            public void add(DeleteRequest... deleteRequests) {

            }

            @Override
            public void add(IndexRequest... indexRequests) {
                for (IndexRequest indexRequest: indexRequests) {
                    bulkProcessor.add(indexRequest);
                }
            }

            @Override
            public void add(UpdateRequest... updateRequests) {

            }
        };
    }

    @Override
    public void open(int i, int i1) {

    }

    @Override
    public void writeRecord(T t) {
        elasticsearchSinkFunction.process(t, getRuntimeContext(), requestIndexer);
    }

    @Override
    public void close() {

    }

}
