 package org.apache.seatunnel.transform.regexparse;

 import com.google.auto.service.AutoService;
 import org.apache.seatunnel.api.configuration.util.OptionRule;
 import org.apache.seatunnel.api.table.connector.TableTransform;
 import org.apache.seatunnel.api.table.factory.Factory;
 import org.apache.seatunnel.api.table.factory.TableTransformFactory;
 import org.apache.seatunnel.api.table.factory.TableTransformFactoryContext;

 @AutoService(Factory.class)
 public class RegexParseTransformFactory implements TableTransformFactory {
    @Override
    public String factoryIdentifier() {
        return "RegexParse";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(RegexParseTransformConfig.REGEX_PARSE_FIELD, RegexParseTransformConfig.REGEX,
 RegexParseTransformConfig.GROUP_MAP)
                .build();
    }

    @Override
    public TableTransform createTransform(TableTransformFactoryContext context) {
        return () -> new RegexParseTransform(context);
    }
 }
