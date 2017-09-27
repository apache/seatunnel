package org.interestinglab.waterdrop.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.ConfigValueFactory;
import org.apache.commons.lang.StringUtils;
import org.interestinglab.waterdrop.configparser.ConfigBaseVisitor;
import org.interestinglab.waterdrop.configparser.ConfigParser;

import java.text.NumberFormat;
import java.text.ParseException;
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

        Config configuration = ConfigFactory.empty();

        if (ctx.sparkBlock() != null) {
            final Config sparkConfigs = visit(ctx.sparkBlock());
            configuration = configuration.withValue("spark", sparkConfigs.root());
        }

        if (ctx.inputBlock() != null) {
            final Config inputPluginList = visit(ctx.inputBlock());
            configuration = configuration.withValue("input", inputPluginList.getValue("input"));
        }

        if (ctx.filterBlock() != null) {
            final Config filtertPluginList = visit(ctx.filterBlock());
            configuration = configuration.withValue("filter", filtertPluginList.getValue("filter"));
        }

        if (ctx.outputBlock() != null) {
            final Config outputPluginList = visit(ctx.outputBlock());
            configuration = configuration.withValue("output", outputPluginList.getValue("output"));
        }

        return configuration;
    }

    @Override
    public Config visitSparkBlock(ConfigParser.SparkBlockContext ctx) {

        if (ctx.entries() != null) {
            return visit(ctx.entries());
        }

        throw new ConfigRuntimeException("Spark Configs must be specified !");
    }

    @Override
    public Config visitOutputBlock(ConfigParser.OutputBlockContext ctx) {

        Config outputPluginList = ConfigFactory.empty();

        List<ConfigValue> statements = new ArrayList<>();
        for (ConfigParser.StatementContext statementContext: ctx.statement()) {

            final Config statement = visit(statementContext);
            if (statement != null && !statement.isEmpty()) {
                statements.add(statement.root());
            }
        }

        outputPluginList = outputPluginList.withValue("output", ConfigValueFactory.fromIterable(statements));
        return outputPluginList;
    }

    @Override
    public Config visitFilterBlock(ConfigParser.FilterBlockContext ctx) {

        Config filterPluginList = ConfigFactory.empty();

        List<ConfigValue> statements = new ArrayList<>();
        for (ConfigParser.StatementContext statementContext: ctx.statement()) {

            final Config statement = visit(statementContext);
            if (statement != null && !statement.isEmpty()) {
                statements.add(statement.root());
            }
        }

        filterPluginList = filterPluginList.withValue("filter", ConfigValueFactory.fromIterable(statements));

        return filterPluginList;
    }

    @Override
    public Config visitInputBlock(ConfigParser.InputBlockContext ctx) {

        Config inputPluginList = ConfigFactory.empty();

        final List<ConfigValue> pluginList = new ArrayList<>();
        for (ConfigParser.PluginContext pluginContext : ctx.plugin()) {

            final Config config = visit(pluginContext);

            if (config != null) {
                pluginList.add(config.root());
            }
        }

        inputPluginList = inputPluginList.withValue("input", ConfigValueFactory.fromIterable(pluginList));
        return inputPluginList;
    }

    @Override
    public Config visitStatement(ConfigParser.StatementContext ctx) {

        Config statement = ConfigFactory.empty();

        if (ctx.plugin() != null) {
            statement = visit(ctx.plugin());
        }

        // TODO: implement if_statement

        return statement;
    }

    @Override
    public Config visitPlugin(ConfigParser.PluginContext ctx) {

        Config plugin = ConfigFactory.empty();

        if (ctx.IDENTIFIER() != null) {

            final String pluginName = ctx.IDENTIFIER().getText();
            plugin = plugin.withValue("name", ConfigValueFactory.fromAnyRef(pluginName));

            if (ctx.entries() != null) {

                final Config entries = visit(ctx.entries());
                if (entries != null && ! entries.isEmpty()) {
                    plugin = plugin.withValue("entries", entries.root());
                } else {
                    plugin = plugin.withValue("entries", ConfigFactory.empty().root());
                }
            }
        }

        return plugin;
    }

    @Override
    public Config visitEntries(ConfigParser.EntriesContext ctx) {

        Config entries = ConfigFactory.empty();
        for (ConfigParser.PairContext pairContext : ctx.pair()) {

            final Config pair = visit(pairContext);
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

            final Config value = visit(ctx.value());

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

            try {
                Number number = NumberFormat.getInstance().parse(ctx.DECIMAL().getText());
                value = ConfigValueFactory.fromAnyRef(number);

            } catch (ParseException e) {
                value = ConfigValueFactory.fromAnyRef(ctx.DECIMAL().getText());
            }

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
        }

        Config config = ConfigFactory.empty();
        config = config.withValue("value", value);
        return config;
    }

    @Override
    public Config visitArray(ConfigParser.ArrayContext ctx) {

        ConfigValue value = ConfigValueFactory.fromAnyRef(null);
        final List<ConfigParser.ValueContext> valueContexts = ctx.value();
        final List<ConfigValue> configValues = new ArrayList<>();
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
