package org.apache.seatunnel.connectors.seatunnel.tdengine.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;
import java.util.Optional;

@Data
@Builder(builderClassName = "Builder")
public class TDengineSinkConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private String url;
    private String username;
    private String password;
    private String database;
    private String stable;
    private String timezone;

    public static TDengineSinkConfig of(ReadonlyConfig config) {
        Builder builder = TDengineSinkConfig.builder();

        builder.url(config.get(TDengineSinkOptions.URL));
        builder.username(config.get(TDengineSinkOptions.USERNAME));
        builder.password(config.get(TDengineSinkOptions.PASSWORD));
        builder.database(config.get(TDengineSinkOptions.DATABASE));
        builder.stable(config.get(TDengineSinkOptions.STABLE));

        Optional<String> optionalTimezone = config.getOptional(TDengineSinkOptions.TIMEZONE);

        builder.timezone(optionalTimezone.orElseGet(TDengineSinkOptions.TIMEZONE::defaultValue));

        return builder.build();
    }
}
