# Add new declarative constraints for numeric range, required cross-field comparison, optional cross-field comparison, and conditional value check
import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.OptionRule;

public class Conditions {
    public static Condition required(String fieldName) {
        return new RequiredCondition(fieldName);
    }

    public static Condition optional(String fieldName) {
        return new OptionalCondition(fieldName);
    }

    public static Condition lessThanField(String fieldName1, String fieldName2) {
        return new LessThanFieldCondition(fieldName1, fieldName2);
    }

    public static Condition lessOrEqualField(String fieldName1, String fieldName2) {
        return new LessOrEqualFieldCondition(fieldName1, fieldName2);
    }

    public static Condition greaterThan(String fieldName, int value) {
        return new GreaterThanCondition(fieldName, value);
    }

    public static Condition conditional(String fieldName, boolean value, Condition condition) {
        return new ConditionalCondition(fieldName, value, condition);
    }

    public interface Condition {
        boolean check(Object value);
    }

    private static class RequiredCondition implements Condition {
        private String fieldName;

        public RequiredCondition(String fieldName) {
            this.fieldName = fieldName;
        }

        @Override
        public boolean check(Object value) {
            return value != null && !value.equals("");
        }
    }

    private static class OptionalCondition implements Condition {
        private String fieldName;

        public OptionalCondition(String fieldName) {
            this.fieldName = fieldName;
        }

        @Override
        public boolean check(Object value) {
            return value != null && !value.equals("");
        }
    }

    private static class LessThanFieldCondition implements Condition {
        private String fieldName1;
        private String fieldName2;

        public LessThanFieldCondition(String fieldName1, String fieldName2) {
            this.fieldName1 = fieldName1;
            this.fieldName2 = fieldName2;
        }

        @Override
        public boolean check(Object value) {
            Object value1 = ((Map<String, Object>) value).get(fieldName1);
            Object value2 = ((Map<String, Object>) value).get(fieldName2);
            return value1 != null && value2 != null && value1 instanceof Number && value2 instanceof Number && ((Number) value1).doubleValue() < ((Number) value2).doubleValue();
        }
    }

    private static class LessOrEqualFieldCondition implements Condition {
        private String fieldName1;
        private String fieldName2;

        public LessOrEqualFieldCondition(String fieldName1, String fieldName2) {
            this.fieldName1 = fieldName1;
            this.fieldName2 = fieldName2;
        }

        @Override
        public boolean check(Object value) {
            Object value1 = ((Map<String, Object>) value).get(fieldName1);
            Object value2 = ((Map<String, Object>) value).get(fieldName2);
            return value1 != null && value2 != null && value1 instanceof Number && value2 instanceof Number && ((Number) value1).doubleValue() <= ((Number) value2).doubleValue();
        }
    }

    private static class GreaterThanCondition implements Condition {
        private String fieldName;
        private int value;

        public GreaterThanCondition(String fieldName, int value) {
            this.fieldName = fieldName;
            this.value = value;
        }

        @Override
        public boolean check(Object value) {
            return value != null && value instanceof Number && ((Number) value).intValue() > value;
        }
    }

    private static class ConditionalCondition implements Condition {
        private String fieldName;
        private boolean value;
        private Condition condition;

        public ConditionalCondition(String fieldName, boolean value, Condition condition) {
            this.fieldName = fieldName;
            this.value = value;
            this.condition = condition;
        }

        @Override
        public boolean check(Object value) {
            return value != null && value.equals(fieldName) && condition.check(value);
        }
    }
}