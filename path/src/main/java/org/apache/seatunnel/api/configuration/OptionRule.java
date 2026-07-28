# Update OptionRule builder to use declarative constraints
import org.apache.seatunnel.api.configuration.util.Conditions;
import org.apache.seatunnel.api.configuration.Option;

public class OptionRule {
    public static OptionRule.Builder builder() {
        return new OptionRule.Builder();
    }

    public static class Builder {
        private OptionRule rule;

        public Builder required(String... fieldNames) {
            rule = new OptionRule();
            for (String fieldName : fieldNames) {
                rule.addCondition(Conditions.required(fieldName));
            }
            return this;
        }

        public Builder optional(String... fieldNames) {
            rule = new OptionRule();
            for (String fieldName : fieldNames) {
                rule.addCondition(Conditions.optional(fieldName));
            }
            return this;
        }

        public Builder conditional(String fieldName, boolean value, Condition condition) {
            rule = new OptionRule();
            rule.addCondition(Conditions.conditional(fieldName, value, condition));
            return this;
        }

        public OptionRule build() {
            return rule;
        }
    }

    public interface Condition {
        boolean check(Object value);
    }

    private List<Condition> conditions = new ArrayList<>();

    public void addCondition(Condition condition) {
        conditions.add(condition);
    }

    public boolean check(Object value) {
        for (Condition condition : conditions) {
            if (!condition.check(value)) {
                return false;
            }
        }
        return true;
    }
}