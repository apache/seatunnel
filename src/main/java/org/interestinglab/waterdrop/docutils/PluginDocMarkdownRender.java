package org.interestinglab.waterdrop.docutils;

import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.WordUtils;
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

        str.append("## " + WordUtils.capitalize(pluginDoc.getPluginGroup()) + " plugin : " + pluginDoc.getPluginName() + "\n\n");
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
        str.append("| --- | --- | --- | --- |\n");
        for (PluginDoc.PluginOption option : pluginDoc.getPluginOptions()) {
            str.append(String.format("| [%s](%s) | %s | %s | %s |\n",
                    option.getOptionName(), getOptionDetailUrl(option), option.getOptionType(), option.getRequired().toString(), option.getDefaultValue()));
        }

        // append option details
        for (PluginDoc.PluginOption option : pluginDoc.getPluginOptions()) {
            str.append("\n");
            str.append("##### " + option.getOptionName() + " [" + option.getOptionType() + "]" + "\n\n");
            str.append(option.getOptionDesc() + "\n");
        }

        return str.toString();
    }

    /**
     * Only for docsify generated docs.
     * */
    private String getOptionDetailUrl(PluginDoc.PluginOption option) {

        final String escapedName = option.getOptionName().replace(".", "");
        return "#" + escapedName + "-" + option.getOptionType();
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

        throw new PluginDocRuntimeException("invalid plugin group in @" + PluginDocLexer.ruleNames[PluginDocLexer.PluginGroup - 1]);
    }

    @Override
    public String visitPluginName(PluginDocParser.PluginNameContext ctx) {

        if (ctx.IDENTIFIER() == null) {
            throw new PluginDocRuntimeException("invalid plugin name in @" + PluginDocLexer.ruleNames[PluginDocLexer.PluginName - 1]);
        }

        return ctx.IDENTIFIER().getText();
    }

    @Override
    public String visitPluginDesc(PluginDocParser.PluginDescContext ctx) {

        if (ctx.IDENTIFIER() == null && ctx.TEXT() == null) {
            throw new PluginDocRuntimeException("invalid plugin description in @" + PluginDocLexer.ruleNames[PluginDocLexer.PluginDesc - 1]);
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
            throw new PluginDocRuntimeException("invalid plugin author in @" + PluginDocLexer.ruleNames[PluginDocLexer.PluginAuthor - 1]);
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
            throw new PluginDocRuntimeException("invalid plugin homepage in @" + PluginDocLexer.ruleNames[PluginDocLexer.PluginHomepage - 1]);
        }

        return ctx.URL().getText();
    }

    @Override
    public String visitPluginVersion(PluginDocParser.PluginVersionContext ctx) {

        if (ctx.VERSION_NUMBER() == null) {
            throw new PluginDocRuntimeException("invalid plugin version in @" + PluginDocLexer.ruleNames[PluginDocLexer.PluginVersion - 1]);
        }

        return ctx.VERSION_NUMBER().getText();
    }

    @Override
    public String visitPluginOption(PluginDocParser.PluginOptionContext ctx) {

        if (ctx.optionType() == null || ctx.optionName() == null || ctx.optionRequired() == null) {
            throw new PluginDocRuntimeException("optionType, optionName, optionRequired shoud be specified");
        }

        final String optionType = visit(ctx.optionType());
        final String optionName = visit(ctx.optionName());
        final String optionRequired = visit(ctx.optionRequired());

        PluginDoc.PluginOption.Required required = PluginDoc.PluginOption.Required.YES;
        if (optionRequired.equals(PluginDoc.PluginOption.Required.NO.toString())) {
            required = PluginDoc.PluginOption.Required.NO;
        }

        String optionDesc = "";
        if (ctx.optionDesc() != null) {
            optionDesc = visit(ctx.optionDesc());
        }

        PluginDoc.PluginOption option = new PluginDoc.PluginOption(optionType, optionName, required, optionDesc);

        if (required == PluginDoc.PluginOption.Required.NO && ctx.optionDefaultValue() != null) {
            option.setDefaultValue(visit(ctx.optionDefaultValue()));
        }

        pluginDoc.getPluginOptions().add(option);
        return null;
    }

    @Override
    public String visitOptionType(PluginDocParser.OptionTypeContext ctx) {

        if (ctx.getText() == null) {
            throw new PluginDocRuntimeException("invalid option type in @" + PluginDocLexer.ruleNames[PluginDocLexer.PluginOption - 1]);
        }

        return ctx.getText();
    }

    @Override
    public String visitOptionName(PluginDocParser.OptionNameContext ctx) {

        if (ctx.IDENTIFIER() == null) {
            throw new PluginDocRuntimeException("invalid option name in @" + PluginDocLexer.ruleNames[PluginDocLexer.PluginOption - 1]);
        }

        return ctx.IDENTIFIER().getText();
    }

    @Override
    public String visitOptionDefaultValue(PluginDocParser.OptionDefaultValueContext ctx) {

        if (ctx.TEXT() == null) {
            throw new PluginDocRuntimeException("invalid option default value in @" + PluginDocLexer.ruleNames[PluginDocLexer.PluginOption - 1]);
        }

        final String unquoted = StringUtils.strip(ctx.TEXT().getText(), "\"'");
        return unquoted;
    }

    @Override
    public String visitOptionRequired(PluginDocParser.OptionRequiredContext ctx) {

        if (ctx.YES() != null) {

            return PluginDoc.PluginOption.Required.YES.toString();

        } else if (ctx.NO() != null) {

            return PluginDoc.PluginOption.Required.NO.toString();

        } else {
            throw new PluginDocRuntimeException("invalid option required in @" + PluginDocLexer.ruleNames[PluginDocLexer.PluginOption - 1]);
        }
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
