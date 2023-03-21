package org.apache.seatunnel.connectors.seatunnel.pulsar.config;

public enum PulsarSemantics {

    /**
     * At this semantics, we will directly send the message to pulsar, the data may duplicat/lost if
     * job restart/retry or network error.
     */
    NON,

    /** At this semantics, we will send at least one */
    AT_LEAST_ONCE,

    /**
     * AT this semantics, we will use 2pc to guarantee the message is sent to pulsar exactly once.
     */
    EXACTLY_ONCE;
}
