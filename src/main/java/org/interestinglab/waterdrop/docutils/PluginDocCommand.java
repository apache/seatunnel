package org.interestinglab.waterdrop.docutils;

import org.antlr.v4.runtime.ANTLRFileStream;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.apache.commons.io.FileUtils;
import org.interestinglab.waterdrop.configparser.*;

import java.io.File;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by gaoyingju on 16/09/2017.
 */
public class PluginDocCommand {

    public static void main(String[] args) throws Exception {

        if (args.length < 1 || args.length > 2) {
            System.out.println("please specify <plugin_doc_path> [overwrite]");
            System.exit(-1);
        }

        boolean overwrite = false;
        if (args.length == 2) {
            overwrite = Boolean.valueOf(args[1]);
        }

        File file = new File(args[0]);
        if (!file.exists() || !file.isFile()) {
            System.out.println("file does not exists or is not file");
            System.exit(-1);
        }

        if (!file.getName().endsWith(".docs")) {
            System.out.println("plugin doc file shoud be named as *.docs");
            System.exit(-1);
        }

        final Path parent = file.toPath().getParent();
        final String filenamePrefix = file.getName().substring(0, file.getName().length() - ".docs".length());
        final File target = new File(Paths.get(parent.toString(), filenamePrefix + ".md").toString());

        if (!overwrite && target.exists()) {
            System.out.println("target rendered plugin doc file already exists");
            System.exit(-1);
        }

        CharStream charStream = new ANTLRFileStream(args[0]);
        // CharStream charStream = CharStreams.fromFileName(args[0]);
        PluginDocLexer lexer = new PluginDocLexer(charStream);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        PluginDocParser parser = new PluginDocParser(tokens);

        PluginDocParser.WaterdropPluginContext context = parser.waterdropPlugin();
        PluginDocVisitor<String> visitor = new PluginDocMarkdownRender();
        String text = visitor.visit(context);

        FileUtils.writeStringToFile(target, text, Charset.defaultCharset());

        System.out.println("Success !");
    }
}
