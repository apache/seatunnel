package org.interestinglab.waterdrop.docutils;

import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.commons.lang.StringUtils;
import org.interestinglab.waterdrop.configparser.PluginDocBaseVisitor;
import org.interestinglab.waterdrop.configparser.PluginDocLexer;
import org.interestinglab.waterdrop.configparser.PluginDocParser;

import java.util.Collections;
import java.util.Comparator;


/**
 * Created by gaoyingju on 16/09/2017.
 */
public class PluginDocMarkdownRender extends PluginDocBaseVisitor<String> {

    private PluginDoc pluginDoc = new PluginDoc();

    private String buildMarkdown() {

        StringBuilder str = new StringBuilder();

        str.append("## " + pluginDoc.getPluginGroup() + " plugin : " + pluginDoc.getPluginName() + "\n\n");
        str.append("* Author: " + pluginDoc.getPluginAuthor() + "\n");
        str.append("* Homepage: " + pluginDoc.getPluginHomepage() + "\n");
        str.append("* Version: " + pluginDoc.getPluginVersion() + "\n");
        str.append("\n");
        str.append("### Description\n\n");
        str.append(pluginDoc.getPluginDesc() + "\n");
        str.append("\n");
        str.append("### Options\n\n");

        Collections.sort(pluginDoc.getPluginOptions(), new Comparator<PluginDoc.PluginOption>() {
            @Override
            public int compare(PluginDoc.PluginOption o1, PluginDoc.PluginOption o2) {
                return o1.getOptionName().compareTo(o2.getOptionName());
            }
        });

        // append markdown table
        str.append("| name | type | required | default value |\n");
        for (PluginDoc.PluginOption option : pluginDoc.getPluginOptions()) {
            str.append(String.format("| %s | %s | %s | %s |\n",
                    option.getOptionName(), option.getOptionType(), option.isRequired(), "null"));
        }

        // append option details
        for (PluginDoc.PluginOption option : pluginDoc.getPluginOptions()) {
            str.append("\n");
            str.append("##### " + option.getOptionName() + " [" + option.getOptionType() + "]" + "\n\n");
            str.append(option.getOptionDesc() + "\n");
        }

        return str.toString();
    }

    @Override
    public String visitWaterdropPlugin(PluginDocParser.WaterdropPluginContext ctx) {

        visitChildren(ctx);
        return buildMarkdown();
    }

    @Override
    public String visitDefinition(PluginDocParser.DefinitionContext ctx) {

        if (ctx.pluginGroup() != null) {

            pluginDoc.setPluginGroup(visit(ctx.pluginGroup()));
        } else if (ctx.pluginName() != null) {

            pluginDoc.setPluginName(visit(ctx.pluginName()));
        } else if (ctx.pluginDesc() != null) {

            pluginDoc.setPluginDesc(visit(ctx.pluginDesc()));
        } else if (ctx.pluginAuthor() != null) {

            pluginDoc.setPluginAuthor(visit(ctx.pluginAuthor()));
        } else if (ctx.pluginHomepage() != null) {

            pluginDoc.setPluginHomepage(visit(ctx.pluginHomepage()));
        } else if (ctx.pluginVersion() != null) {

            pluginDoc.setPluginVersion(visit(ctx.pluginVersion()));
        } else if (ctx.pluginOption() != null) {

            visit(ctx.pluginOption());
        }

        return null;
    }

    @Override
    public String visitPluginGroup(PluginDocParser.PluginGroupContext ctx) {

        if (ctx.INPUT() != null) {
            return ctx.INPUT().getText();
        } else if (ctx.FILTER() != null) {
            return ctx.FILTER().getText();
        } else if (ctx.OUTPUT() != null) {
            return ctx.OUTPUT().getText();
        }

        throw new RuntimeException("visitPluginGroup 1");
    }

    @Override
    public String visitPluginName(PluginDocParser.PluginNameContext ctx) {

        if (ctx.IDENTIFIER() == null) {
            throw new RuntimeException("visitPluginName 1");
        }

        return ctx.IDENTIFIER().getText();
    }

    @Override
    public String visitPluginDesc(PluginDocParser.PluginDescContext ctx) {

        if (ctx.IDENTIFIER() == null && ctx.TEXT() == null) {
            throw new RuntimeException("visitPluginDesc 1");
        }

        TerminalNode node = ctx.IDENTIFIER();
        if (node == null) {
            node = ctx.TEXT();
        }

        final String unquoted = StringUtils.strip(node.getText(), "\"'");
        return unquoted;
    }

    @Override
    public String visitPluginAuthor(PluginDocParser.PluginAuthorContext ctx) {

        if (ctx.TEXT() == null && ctx.IDENTIFIER() == null) {
            throw new RuntimeException("visitPluginAuthor 1");
        }

        TerminalNode node = ctx.TEXT();
        if (node == null) {
            node = ctx.IDENTIFIER();
        }

        final String unquoted = StringUtils.strip(node.getText(), "\"'");
        return unquoted;
    }

    @Override
    public String visitPluginHomepage(PluginDocParser.PluginHomepageContext ctx) {

        if (ctx.URL() == null) {
            throw new RuntimeException("visitPluginHomepage 1");
        }

        return ctx.URL().getText();
    }

    @Override
    public String visitPluginVersion(PluginDocParser.PluginVersionContext ctx) {

        if (ctx.VERSION_NUMBER() == null) {
            throw new RuntimeException("visitPluginVersion 1");
        }

        return ctx.VERSION_NUMBER().getText();
    }

    @Override
    public String visitPluginOption(PluginDocParser.PluginOptionContext ctx) {

        if (ctx.optionType() == null || ctx.optionName() == null) {
            throw new RuntimeException("visitPluginOption 1");
        }

        final String optionType = visit(ctx.optionType());
        final String optionName = visit(ctx.optionName());
        String optionDesc = "";
        if (ctx.optionDesc() != null) {
            optionDesc = visit(ctx.optionDesc());
        }

        PluginDoc.PluginOption option = new PluginDoc.PluginOption(optionType, optionName, optionDesc);
        pluginDoc.getPluginOptions().add(option);

        return null;
    }

    @Override
    public String visitOptionType(PluginDocParser.OptionTypeContext ctx) {

        if (ctx.getText() == null) {
            throw new RuntimeException("invalid option type in @" + PluginDocLexer.ruleNames[PluginDocLexer.PluginOption - 1]);
        }

        return ctx.getText();
    }

    @Override
    public String visitOptionName(PluginDocParser.OptionNameContext ctx) {

        if (ctx.IDENTIFIER() == null) {
            throw new RuntimeException("invalid option name in @" + PluginDocLexer.ruleNames[PluginDocLexer.PluginOption - 1]);
        }

        return ctx.IDENTIFIER().getText();
    }

    @Override
    public String visitOptionDesc(PluginDocParser.OptionDescContext ctx) {

        if (ctx.TEXT() != null) {

            final String unquoted = StringUtils.strip(ctx.TEXT().getText(), "\"'");
            return unquoted;
        }

        return "";
    }
}
