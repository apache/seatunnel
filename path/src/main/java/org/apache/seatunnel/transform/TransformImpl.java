# Update transform implementation to use declarative constraints
import org.apache.seatunnel.api.configuration.OptionRule;
import org.apache.seatunnel.api.configuration.Conditions;

public class TransformImpl extends Transform {
    public TransformImpl() {
        super();
    }

    public boolean validate(Map<String, Object> config) {
        return super.validate(config);
    }
}