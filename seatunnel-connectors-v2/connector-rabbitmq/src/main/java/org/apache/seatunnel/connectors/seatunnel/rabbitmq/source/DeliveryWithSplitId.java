package org.apache.seatunnel.connectors.seatunnel.rabbitmq.source;

import com.rabbitmq.client.Delivery;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@AllArgsConstructor
@Setter
@Getter
public final class DeliveryWithSplitId {
    private final Delivery delivery;
    private final String splitId;
}
