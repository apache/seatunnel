package org.apache.seatunnel.connectors.seatunnel.mongodb.serde;

import java.io.Serializable;
import java.util.function.Function;

@FunctionalInterface
public interface SerializableFunction<T, R> extends Function<T, R>, Serializable {}
