package io.github.interestinglab.waterdrop;

import io.github.interestinglab.waterdrop.config.CommandLineArgs;
import io.github.interestinglab.waterdrop.config.CommandLineUtils;
import scopt.OptionParser;


/**
 * @author mr_xiong
 * @date 2019-12-29 16:15
 * @description
 */
public class WaterdropFlink {
    private static final String FLINK = "flink";

    public static void main(String[] args) {
        OptionParser<CommandLineArgs> flinkParser = CommandLineUtils.flinkParser();
        Waterdrop.run(flinkParser, FLINK, args);
    }

}
