package org.apache.seatunnel.connectors.seatunnel.kafka.config;

public enum KafkaSemantics {

    /**
     * At this semantics, we will directly send the message to kafka, the data may duplicat/lost
     * if job restart/retry or network error.
     */
    NON,

    /**
     * At this semantics, we will retry sending the message to kafka, if the response is not ack.
     */
    AT_LEAST_ONCE,

    /**
     * AT this semantics, we will use 2pc to guarantee the message is sent to kafka exactly once.
     */
    EXACTLY_ONCE,
    ;


}
