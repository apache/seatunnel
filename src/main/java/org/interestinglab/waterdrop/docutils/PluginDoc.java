package org.interestinglab.waterdrop.docutils;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by gaoyingju on 17/09/2017.
 */
public class PluginDoc {

    private String pluginGroup;
    private String pluginName;
    private String pluginDesc;
    private String pluginAuthor;
    private String pluginHomepage;
    private String pluginVersion;
    private List<PluginOption> pluginOptions = new ArrayList<>();

    public static class PluginOption {

        private String optionType;
        private String optionName;
        private String optionDesc = "";
        private Required required = Required.YES;
        private String defaultValue = "-";

        public enum Required {
            YES("yes"),
            NO("no")
            ;

            private final String text;

            /**
             * @param text
             */
            Required(final String text) {
                this.text = text;
            }

            /* (non-Javadoc)
             * @see java.lang.Enum#toString()
             */
            @Override
            public String toString() {
                return text;
            }
        }

        public PluginOption(final String optionType, final String optionName, final Required required, final String optionDesc) {
            this.optionType = optionType;
            this.optionName = optionName;
            this.required = required;
            this.optionDesc = optionDesc;
        }

        public String getOptionType() {
            return optionType;
        }

        public void setOptionType(String optionType) {
            this.optionType = optionType;
        }

        public String getOptionName() {
            return optionName;
        }

        public void setOptionName(String optionName) {
            this.optionName = optionName;
        }

        public String getOptionDesc() {
            return optionDesc;
        }

        public void setOptionDesc(String optionDesc) {
            this.optionDesc = optionDesc;
        }

        public Required getRequired() {
            return required;
        }

        public void setRequired(Required required) {
            this.required = required;
        }

        public String getDefaultValue() {
            return defaultValue;
        }

        public void setDefaultValue(String defaultValue) {
            this.defaultValue = defaultValue;
        }
    }

    public String getPluginGroup() {
        return pluginGroup;
    }

    public void setPluginGroup(String pluginGroup) {
        this.pluginGroup = pluginGroup;
    }

    public String getPluginName() {
        return pluginName;
    }

    public void setPluginName(String pluginName) {
        this.pluginName = pluginName;
    }

    public String getPluginDesc() {
        return pluginDesc;
    }

    public void setPluginDesc(String pluginDesc) {
        this.pluginDesc = pluginDesc;
    }

    public String getPluginAuthor() {
        return pluginAuthor;
    }

    public void setPluginAuthor(String pluginAuthor) {
        this.pluginAuthor = pluginAuthor;
    }

    public String getPluginHomepage() {
        return pluginHomepage;
    }

    public void setPluginHomepage(String pluginHomepage) {
        this.pluginHomepage = pluginHomepage;
    }

    public String getPluginVersion() {
        return pluginVersion;
    }

    public void setPluginVersion(String pluginVersion) {
        this.pluginVersion = pluginVersion;
    }

    public List<PluginOption> getPluginOptions() {
        return pluginOptions;
    }

    public void setPluginOptions(List<PluginOption> pluginOptions) {
        this.pluginOptions = pluginOptions;
    }
}
