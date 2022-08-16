
package org.apache.seatunnel.connectors.seatunnel.neo4j.sink;

import com.google.auto.service.AutoService;
import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelContext;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SinkCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.neo4j.config.DriverBuilder;
import org.apache.seatunnel.connectors.seatunnel.neo4j.config.Neo4jConfig;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.neo4j.driver.AuthTokens;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Optional;

import static org.apache.seatunnel.connectors.seatunnel.neo4j.config.Neo4jConfig.*;

@AutoService(SeaTunnelSink.class)
public class Neo4jSink implements SeaTunnelSink<SeaTunnelRow, Neo4jState, Neo4jCommitInfo, Neo4jAggregatedCommitInfo> {

    private SeaTunnelRowType rowType;
    private final Neo4jConfig neo4jConfig = new Neo4jConfig();

    @Override
    public String getPluginName() {
        return KEY_SINK_PLUGIN_NAME;
    }

    @Override
    public void prepare(Config config) throws PrepareFailException {
        neo4jConfig.setDriverBuilder(prepareDriver(config));

        CheckConfigUtil.checkAllExists(config, KEY_QUERY, KEY_QUERY_PARAM_POSITION);
        neo4jConfig.setQuery(config.getString(KEY_QUERY));
        neo4jConfig.setQueryParamPosition(config.getObject(KEY_QUERY_PARAM_POSITION).unwrapped());
    }

    private DriverBuilder prepareDriver(Config config) {
        final CheckResult uriParamCheck = CheckConfigUtil.checkAllExists(config, KEY_NEO4J_URI);
        final CheckResult authParamCheck = CheckConfigUtil.checkAtLeastOneExists(config, KEY_USERNAME, KEY_BEARER_TOKEN, KEY_KERBEROS_TICKET);
        final CheckResult paramCheck = CheckConfigUtil.mergeCheckResults(uriParamCheck, authParamCheck);
        if (!paramCheck.isSuccess()) {
            throw new PrepareFailException(KEY_SINK_PLUGIN_NAME, PluginType.SINK, paramCheck.getMsg());
        }

        final URI uri = URI.create(config.getString(KEY_NEO4J_URI));
        if (!"neo4j".equals(uri.getScheme())) {
            throw new PrepareFailException(KEY_SINK_PLUGIN_NAME, PluginType.SINK, "uri scheme is not `neo4j`");
        }

        final DriverBuilder driverBuilder = DriverBuilder.create(uri);

        if (config.hasPath(KEY_USERNAME)) {
            final CheckResult pwParamCheck = CheckConfigUtil.checkAllExists(config, KEY_PASSWORD);
            if (!paramCheck.isSuccess()) {
                throw new PrepareFailException(KEY_SINK_PLUGIN_NAME, PluginType.SINK, pwParamCheck.getMsg());
            }
            final String username = config.getString(KEY_USERNAME);
            final String password = config.getString(KEY_PASSWORD);

            driverBuilder.setUsername(username);
            driverBuilder.setPassword(password);
        } else if (config.hasPath(KEY_BEARER_TOKEN)) {
            final String bearerToken = config.getString(KEY_BEARER_TOKEN);
            AuthTokens.bearer(bearerToken);
            driverBuilder.setBearerToken(bearerToken);
        } else {
            final String kerberosTicket = config.getString(KEY_KERBEROS_TICKET);
            AuthTokens.kerberos(kerberosTicket);
            driverBuilder.setBearerToken(kerberosTicket);
        }
        return driverBuilder;
    }

    @Override
    public void setTypeInfo(SeaTunnelRowType seaTunnelRowType) {
        this.rowType = seaTunnelRowType;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getConsumedType() {
        return this.rowType;
    }

    @Override
    public SinkWriter<SeaTunnelRow, Neo4jCommitInfo, Neo4jState> createWriter(SinkWriter.Context context) throws IOException {
        return new Neo4jSinkWriter(neo4jConfig);
    }

    @Override
    public SinkWriter<SeaTunnelRow, Neo4jCommitInfo, Neo4jState> restoreWriter(SinkWriter.Context context, List<Neo4jState> states) throws IOException {
        return SeaTunnelSink.super.restoreWriter(context, states);
    }

    @Override
    public Optional<Serializer<Neo4jState>> getWriterStateSerializer() {
        return SeaTunnelSink.super.getWriterStateSerializer();
    }

    @Override
    public Optional<SinkCommitter<Neo4jCommitInfo>> createCommitter() throws IOException {
        return SeaTunnelSink.super.createCommitter();
    }

    @Override
    public Optional<Serializer<Neo4jCommitInfo>> getCommitInfoSerializer() {
        return SeaTunnelSink.super.getCommitInfoSerializer();
    }

    @Override
    public Optional<SinkAggregatedCommitter<Neo4jCommitInfo, Neo4jAggregatedCommitInfo>> createAggregatedCommitter() throws IOException {
        return SeaTunnelSink.super.createAggregatedCommitter();
    }

    @Override
    public Optional<Serializer<Neo4jAggregatedCommitInfo>> getAggregatedCommitInfoSerializer() {
        return SeaTunnelSink.super.getAggregatedCommitInfoSerializer();
    }

    @Override
    public void setSeaTunnelContext(SeaTunnelContext seaTunnelContext) {
        SeaTunnelSink.super.setSeaTunnelContext(seaTunnelContext);
    }
}
