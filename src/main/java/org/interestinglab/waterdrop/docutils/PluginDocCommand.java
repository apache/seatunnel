package org.interestinglab.waterdrop.docutils;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.interestinglab.waterdrop.configparser.*;

/**
 * Created by gaoyingju on 16/09/2017.
 */
public class PluginDocCommand {

    public static void main(String[] args) throws Exception {

        CharStream charStream = CharStreams.fromFileName(args[0]);
        PluginDocLexer lexer = new PluginDocLexer(charStream);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        PluginDocParser parser = new PluginDocParser(tokens);

        PluginDocParser.WaterdropPluginContext context = parser.waterdropPlugin();
        PluginDocVisitor<String> visitor = new PluginDocMarkdownRender();
        String text = visitor.visit(context);

        System.out.println("Rendered Plugin Doc: \n" + text);
    }
}
