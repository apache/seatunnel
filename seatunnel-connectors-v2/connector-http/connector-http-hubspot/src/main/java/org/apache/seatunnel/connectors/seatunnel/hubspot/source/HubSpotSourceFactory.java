package org.apache.seatunnel.connectors.seatunnel.hubspot.source;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;

import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class HubSpotSourceFactory implements TableSourceFactory {

    // Define the options the user will put in their config file
    public static final Option<String> ACCESS_TOKEN =
            Options.key("access_token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("HubSpot Private App Access Token");

    public static final Option<String> OBJECT_TYPE =
            Options.key("object_type")
                    .stringType()
                    .defaultValue("contacts")
                    .withDescription("The HubSpot object to fetch (contacts, companies, deals)");

    @Override
    public String factoryIdentifier() {
        return "HubSpot";
    }

    @Override
    public OptionRule optionRule() {
        // This tells SeaTunnel: "If the user uses HubSpot, they MUST provide an access_token"
        return OptionRule.builder().required(ACCESS_TOKEN).optional(OBJECT_TYPE).build();
    }

    @Override
    @SuppressWarnings("rawtypes")
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return HubSpotSource.class;
    }
}
