package org.apache.seatunnel.connectors.seatunnel.mongodbv2.serde;

import org.bson.Document;

import java.io.Serializable;

/** DocumentSerializer serialize POJOs or other Java objects into {@link Document}. */
public interface DocumentSerializer<T> extends Serializable {

    /**
     * Serialize input Java objects into {@link Document}.
     *
     * @param object The input object.
     * @return The serialized {@link Document}.
     */
    Document serialize(T object);
}
