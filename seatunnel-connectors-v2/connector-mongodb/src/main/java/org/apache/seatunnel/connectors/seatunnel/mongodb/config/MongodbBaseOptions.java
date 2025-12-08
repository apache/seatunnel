package org.apache.seatunnel.connectors.seatunnel.mongodb.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;

public class MongodbBaseOptions {

    public static final String ENCODE_VALUE_FIELD = "_value";

    public static final JsonWriterSettings DEFAULT_JSON_WRITER_SETTINGS =
            JsonWriterSettings.builder().outputMode(JsonMode.EXTENDED).build();

    public static final String CONNECTOR_IDENTITY = "MongoDB";

    public static final Option<String> URI =
            Options.key("uri")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The MongoDB connection uri.");

    public static final Option<String> DATABASE =
            Options.key("database")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The name of MongoDB database to read or write.");

    public static final Option<String> COLLECTION =
            Options.key("collection")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The name of MongoDB collection to read or write.");
}
