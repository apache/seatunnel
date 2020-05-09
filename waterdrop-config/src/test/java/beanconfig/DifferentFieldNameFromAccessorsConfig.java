package beanconfig;

public class DifferentFieldNameFromAccessorsConfig {

    private String customStringField;
    private Long number;


    public String getStringField() {
        return customStringField;
    }

    public void setStringField(String stringField) {
        this.customStringField = stringField;
    }

    public Long getNumber() {
        return number;
    }

    public void setNumber(Long number) {
        this.number = number;
    }
}
