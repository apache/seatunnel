package org.interestinglab.waterdrop.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.ConfigValueFactory;
import org.apache.commons.lang.StringUtils;
import org.interestinglab.waterdrop.configparser.ConfigBaseVisitor;
import org.interestinglab.waterdrop.configparser.ConfigParser;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

// TODO: visit if_statement

/**
 * Created by gaoyingju on 11/09/2017.
 */
public class ConfigVisitorImpl extends ConfigBaseVisitor<Config> {

    @Override
    public Config visitConfig(ConfigParser.ConfigContext ctx) {
        // TODO
        return null;
    }

    @Override
    public Config visitOutput_block(ConfigParser.Output_blockContext ctx) {
        // TODO
        return null;
    }

    @Override
    public Config visitFilter_block(ConfigParser.Filter_blockContext ctx) {
        // TODO:
        return null;
    }

    @Override
    public Config visitInput_block(ConfigParser.Input_blockContext ctx) {
        // TODO:
        return null;
    }

    @Override
    public Config visitStatement(ConfigParser.StatementContext ctx) {
        // TODO:
        return null;
    }

    @Override
    public Config visitPlugin(ConfigParser.PluginContext ctx) {

        Config plugin = ConfigFactory.empty();

        if (ctx.IDENTIFIER() != null) {

            String pluginName = ctx.IDENTIFIER().getText();
            plugin = plugin.withValue("name", ConfigValueFactory.fromAnyRef(pluginName));

            if (ctx.entries() != null) {

                Config entries = visit(ctx.entries());
                if (entries != null && ! entries.isEmpty()) {
                    plugin = plugin.withValue("entries", entries.root());
                }
            }
        }

        return plugin;
    }

    @Override
    public Config visitEntries(ConfigParser.EntriesContext ctx) {

        Config entries = ConfigFactory.empty();

        List<ConfigValue> configValues = new ArrayList<>();
        for (ConfigParser.PairContext pairContext : ctx.pair()) {

            Config pair = visit(pairContext);
            if (pair != null && ! pair.isEmpty()) {

                for (Map.Entry<String, ConfigValue> entry : pair.entrySet()) {
                    entries = entries.withValue(entry.getKey(), entry.getValue());
                }
            }
        }

        return entries;
    }

    @Override
    public Config visitPair(ConfigParser.PairContext ctx) {

        Config pair = ConfigFactory.empty();

        if (ctx.IDENTIFIER() != null) {

            Config value = visit(ctx.value());

            if (value != null && value.hasPath("value")) {
                pair = pair.withValue(ctx.IDENTIFIER().getText(), value.getValue("value"));
            }
        }

        return pair;
    }

    @Override
    public Config visitValue(ConfigParser.ValueContext ctx) {

        ConfigValue value = ConfigValueFactory.fromAnyRef(null);

        if (ctx.DECIMAL() != null) {

            // TODO: java.lang.Number, config.getNumber()
        } else if (ctx.QUOTED_STRING() != null) {

            final String unquoted = StringUtils.strip(ctx.QUOTED_STRING().getText(), "\"'");
            value = ConfigValueFactory.fromAnyRef(unquoted);

        } else if (ctx.array() != null) {

            Config array = visit(ctx.array());
            if (array != null) {
                value = array.getValue("value");
            }

        } else if (ctx.TRUE() != null) {

            value = ConfigValueFactory.fromAnyRef(true);

        } else if (ctx.FALSE() != null) {

            value = ConfigValueFactory.fromAnyRef(false);

        } else if (ctx.NULL() != null) {
        }

        Config config = ConfigFactory.empty();
        config = config.withValue("value", value);
        return config;
    }

    @Override
    public Config visitArray(ConfigParser.ArrayContext ctx) {

        ConfigValue value = ConfigValueFactory.fromAnyRef(null);
        List<ConfigParser.ValueContext> valueContexts = ctx.value();
        List<ConfigValue> configValues = new ArrayList<>();
        for (ConfigParser.ValueContext valueContext : valueContexts) {

            Config config = visit(valueContext);
            if (config != null) {
                configValues.add(config.getValue("value"));
            }
        }

        if (configValues.size() > 0) {
            value = ConfigValueFactory.fromIterable(configValues);
        }

        Config config = ConfigFactory.empty();
        config = config.withValue("value", value);
        return config;
    }
}
