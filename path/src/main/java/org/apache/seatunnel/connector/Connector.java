# Update connector validation logic to use declarative constraints
import org.apache.seatunnel.api.configuration.OptionRule;
import org.apache.seatunnel.api.configuration.Conditions;

public class Connector {
    private OptionRule optionRule;

    public Connector() {
        optionRule = OptionRule.builder()
                .required("port", Conditions.greaterThan("port", 0))
                .build();
    }

    public boolean validate(Map<String, Object> config) {
        return optionRule.check(config);
    }
}