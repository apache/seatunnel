package org.apache.seatunnel.connectors.seatunnel.http.source;

import static org.apache.seatunnel.connectors.seatunnel.http.client.HttpResponse.STATUS_OK;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.http.client.HttpClientProvider;
import org.apache.seatunnel.connectors.seatunnel.http.client.HttpResponse;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

public class HttpSourceReader implements SourceReader<SeaTunnelRow, HttpSourceSplit> {

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpSourceReader.class);
    private final SourceReader.Context context;
    private final HttpSourceParameter parameter;
    private final HttpSourceParameter.ScheduleParameter scheduleParameter;
    private HttpClientProvider httpClient;
    private ScheduledExecutorService executorService;

    public HttpSourceReader(Context context, HttpSourceParameter parameter) {
        this.context = context;
        this.parameter = parameter;
        this.scheduleParameter = parameter.toScheduleParameter();
    }

    @Override
    public void open() {
        httpClient = HttpClientProvider.getInstance();
        executorService = new ScheduledThreadPoolExecutor(1,
                new BasicThreadFactory
                        .Builder()
                        .namingPattern("http-source-%d")
                        .daemon(true).build());
    }

    @Override
    public void close() throws IOException {
        if (Objects.nonNull(executorService)) {
            executorService.shutdown();
        }
        if (Objects.nonNull(httpClient)) {
            httpClient.close();
        }
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        executorService.scheduleAtFixedRate(() -> execute(output), 0, scheduleParameter.getPeriod(), scheduleParameter.getUnit());
    }

    private void execute(Collector<SeaTunnelRow> output) {
        try {
            HttpResponse response = httpClient.execute(this.parameter.getUrl(), this.parameter.getMethod(), this.parameter.getHeadersMap(), this.parameter.getParamsMap());
            if (STATUS_OK == response.getCode()) {
                output.collect(new SeaTunnelRow(new Object[] {response.getContent()}));
                return;
            }
            LOGGER.error("http client execute exception, http response status code:[{}], content:[{}]", response.getCode(), response.getContent());
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }

    }

    @Override
    public List<HttpSourceSplit> snapshotState(long checkpointId) throws Exception {
        return null;
    }

    @Override
    public void addSplits(List<HttpSourceSplit> splits) {

    }

    @Override
    public void handleNoMoreSplits() {

    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {

    }
}
