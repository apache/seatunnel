# Update connector implementation to use declarative constraints
import org.apache.seatunnel.api.configuration.OptionRule;
import org.apache.seatunnel.api.configuration.Conditions;

public class ConnectorImpl extends Connector {
    public ConnectorImpl() {
        super();
    }

    public boolean validate(Map<String, Object> config) {
        return super.validate(config);
    }
}