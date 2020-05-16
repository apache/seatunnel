package beanconfig;

public class NotABeanFieldConfig {

    public static class NotABean {
        int stuff;
    }

    private NotABean notBean;

    public NotABean getNotBean() {
        return notBean;
    }

    public void setNotBean(NotABean notBean) {
        this.notBean = notBean;
    }
}
