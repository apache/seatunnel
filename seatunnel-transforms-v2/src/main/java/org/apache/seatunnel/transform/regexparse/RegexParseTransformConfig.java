 package org.apache.seatunnel.transform.regexparse;

 import org.apache.seatunnel.api.configuration.Option;
 import org.apache.seatunnel.api.configuration.Options;

 import java.io.Serializable;
 import java.util.Map;


 public class RegexParseTransformConfig implements Serializable {
    private static final long serialVersionUID = -930897758226053570L;
    public static final Option<String> REGEX_PARSE_FIELD =
            Options.key("regex_parse_field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Upstream field that requires parsing");
    public static final Option<String> REGEX =
            Options.key("regex")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "regular expression");
    public static final Option<Map<String, String>> GROUP_MAP =
            Options.key("groupMap")
                    .mapType()
                    .noDefaultValue()
                    .withDescription(
                            "The correspondence between result fields and regular capture group indexes");


 }
