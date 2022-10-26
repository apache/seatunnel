package org.apache.seatunnel.connectors.seatunnel.rabbitmq.source;

import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.Delivery;
import org.apache.seatunnel.common.Handover;
import org.apache.seatunnel.connectors.seatunnel.pulsar.source.reader.PulsarSplitReaderThread;
import org.apache.seatunnel.connectors.seatunnel.pulsar.source.reader.RecordWithSplitId;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.client.QueueingConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RabbitMQSplitReaderThread extends Thread implements Cloneable {
    private static final Logger LOG = LoggerFactory.getLogger(RabbitMQSplitReaderThread.class);
    protected Consumer consumer;
    protected final Handover<Delivery> handover;
    private volatile boolean running;

    public RabbitMQSplitReaderThread(Handover<Delivery> handover) {
        this.handover = handover;
    }


    public void open(Handover<Delivery> handover) {
        consumer = new QueueingConsumer(null);

    }

    @Override
    public void run() {
        while (running) {

        }

    }
}
