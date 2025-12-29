package org.apache.seatunnel.api.configuration.util.issue;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.common.constants.PluginType;

public class DeprecatedConfigurationIssue extends ConfigurationVerificationIssue{


    private DeprecatedConfigurationIssue(Level level, String identifier, PluginType pluginType, Option<?> option) {
        super(level, identifier, pluginType, option);

    }

    @Override
    public void log() {

    }


}
