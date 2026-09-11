import org.apache.seatunnel.connectors.seatunnel.linear.LinearSourceFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class LinearSourceFactoryTest {
    @Test
    public void testFactoryIdentifier() {
        LinearSourceFactory factory = new LinearSourceFactory();
        Assertions.assertEquals("Linear", factory.factoryIdentifier());
    }
}
