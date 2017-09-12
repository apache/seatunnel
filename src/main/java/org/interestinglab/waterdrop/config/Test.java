package org.interestinglab.waterdrop.config;

import com.typesafe.config.Config;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.interestinglab.waterdrop.configparser.ConfigLexer;
import org.interestinglab.waterdrop.configparser.ConfigParser;
import org.interestinglab.waterdrop.configparser.ConfigVisitor;

/**
 * Created by gaoyingju on 11/09/2017.
 */
public class Test {

    public static void main(String[] args) throws Exception {

        CharStream charStream = CharStreams.fromFileName(args[0]);
        ConfigLexer lexer = new ConfigLexer(charStream);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        ConfigParser parser = new ConfigParser(tokens);

        ConfigParser.ConfigContext configContext = parser.config();
        ConfigVisitor<Config> visitor = new ConfigVisitorImpl();
        System.out.println(visitor.visit(configContext));
    }
}
