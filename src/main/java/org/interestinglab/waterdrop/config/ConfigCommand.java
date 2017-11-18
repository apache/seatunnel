package org.interestinglab.waterdrop.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigRenderOptions;
import org.antlr.v4.runtime.ANTLRFileStream;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.interestinglab.waterdrop.configparser.ConfigLexer;
import org.interestinglab.waterdrop.configparser.ConfigParser;
import org.interestinglab.waterdrop.configparser.ConfigVisitor;

/**
 * Created by gaoyingju on 11/09/2017.
 */
public class ConfigCommand {

    /**
     * sbt "run-main org.interestinglab.waterdrop.config.ConfigCommand src/main/antlr4/example.conf"
     * */
    public static void main(String[] args) throws Exception {

        CharStream charStream = new ANTLRFileStream(args[0]);
        // CharStream charStream = CharStreams.fromFileName();
        ConfigLexer lexer = new ConfigLexer(charStream);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        ConfigParser parser = new ConfigParser(tokens);

        ConfigParser.ConfigContext configContext = parser.config();
        ConfigVisitor<Config> visitor = new ConfigVisitorImpl();
        Config appConfig = visitor.visit(configContext);

        ConfigRenderOptions options = ConfigRenderOptions.concise().setFormatted(true);
        System.out.println(appConfig.root().render(options));

        System.out.println("List Filter Plugins: ");
        for (Config filter : appConfig.getConfigList("filter")) {

            System.out.println(filter.root().render(options));
        }
    }
}
