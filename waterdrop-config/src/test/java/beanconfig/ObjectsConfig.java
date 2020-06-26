package beanconfig;


import io.github.interestinglab.waterdrop.config.Optional;

public class ObjectsConfig {
    public static class ValueObject {
        @Optional
        private String optionalValue;
        private String mandatoryValue;

        public String getMandatoryValue() {
          return mandatoryValue;
        }

        public void setMandatoryValue(String mandatoryValue) {
          this.mandatoryValue = mandatoryValue;
        }

        public String getOptionalValue() {
          return optionalValue;
        }

        public void setOptionalValue(String optionalValue) {
          this.optionalValue = optionalValue;
        }
    }

    private ValueObject valueObject;

    public ValueObject getValueObject() {

        return valueObject;
    }

    public void setValueObject(ValueObject valueObject) {
        this.valueObject = valueObject;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ObjectsConfig)) {
            return false;
        }

        ObjectsConfig that = (ObjectsConfig) o;

        return !(getValueObject() != null ? !getValueObject().equals(that.getValueObject()) : that.getValueObject() != null);

    }

    @Override
    public int hashCode() {
        return getValueObject() != null ? getValueObject().hashCode() : 0;
    }


    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("ObjectsConfig{");
        sb.append("innerType=").append(valueObject);
        sb.append('}');
        return sb.toString();
    }
}
